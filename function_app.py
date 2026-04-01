import os
import logging
import traceback
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def harvest_time_entries_incremental(mytimer: func.TimerRequest) -> None:
    logging.error("=== FUNCTION STARTED ===")

    try:
        import requests
        import pandas as pd
        from mssql_python import connect

        logging.error("Step 1 OK: modules imported")

        # Clear proxy env vars
        for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"]:
            os.environ.pop(key, None)

        server = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        username = os.getenv("AZURE_SQL_USERNAME")
        password = os.getenv("AZURE_SQL_PASSWORD")
        harvest_account_id = os.getenv("HARVEST_ACCOUNT_ID")
        harvest_token = os.getenv("HARVEST_TOKEN")

        logging.error(f"AZURE_SQL_SERVER present: {bool(server)}")
        logging.error(f"AZURE_SQL_DATABASE present: {bool(database)}")
        logging.error(f"AZURE_SQL_USERNAME present: {bool(username)}")
        logging.error(f"AZURE_SQL_PASSWORD present: {bool(password)}")
        logging.error(f"HARVEST_ACCOUNT_ID present: {bool(harvest_account_id)}")
        logging.error(f"HARVEST_TOKEN present: {bool(harvest_token)}")

        if not all([server, database, username, password, harvest_account_id, harvest_token]):
            raise ValueError("One or more required environment variables are missing.")

        # Build SQL connection string for mssql-python
        sql_connection_string = (
            f"Server={server};"
            f"Database={database};"
            f"UID={username};"
            f"PWD={password};"
            "Authentication=SqlPassword;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
)

        logging.error("Step 2: opening SQL connection")
        conn = connect(sql_connection_string)
        cursor = conn.cursor()

        # Test connection
        cursor.execute("SELECT 1")
        cursor.fetchone()
        logging.error("Step 2 OK: DB connection successful")

        # Read last sync time
        cursor.execute("""
            SELECT last_successful_sync_utc
            FROM rpt.sync_state
            WHERE source_name = 'harvest_time_entries'
        """)
        row = cursor.fetchone()
        last_sync = row[0] if row else None
        logging.error(f"Step 3 OK: last_sync = {last_sync}")

        # Harvest request
        headers = {
            "Authorization": f"Bearer {harvest_token}",
            "Harvest-Account-ID": harvest_account_id,
            "User-Agent": "jayva-harvest-function"
        }

        session = requests.Session()
        session.trust_env = False
        session.proxies = {}

        page = 1
        all_rows = []

        while True:
            response = session.get(
                "https://api.harvestapp.com/v2/time_entries",
                headers=headers,
                params={
                    "updated_since": last_sync.isoformat() if last_sync else "2025-01-01T00:00:00Z",
                    "page": page,
                    "per_page": 2000
                },
                timeout=60
            )
            logging.error(f"Harvest response status: {response.status_code}")
            response.raise_for_status()

            payload = response.json()
            entries = payload.get("time_entries", [])

            if not entries:
                break

            for e in entries:
                project = e.get("project") or {}
                task = e.get("task") or {}
                user = e.get("user") or {}
                client = e.get("client") or {}

                all_rows.append((
                    e.get("id"),
                    e.get("spent_date"),
                    e.get("hours"),
                    e.get("rounded_hours"),
                    e.get("notes"),
                    e.get("is_billed"),
                    e.get("billable"),
                    project.get("id"),
                    project.get("name"),
                    task.get("id"),
                    task.get("name"),
                    user.get("id"),
                    user.get("name"),
                    client.get("id"),
                    client.get("name"),
                    e.get("created_at"),
                    e.get("updated_at"),
                ))

            total_pages = payload.get("total_pages", page)
            if page >= total_pages:
                break
            page += 1

        logging.error(f"Step 4 OK: pulled {len(all_rows)} rows from Harvest")

        if not all_rows:
            logging.error("No new rows returned from Harvest. Updating sync timestamp only.")
            cursor.execute("""
                UPDATE rpt.sync_state
                SET last_successful_sync_utc = SYSUTCDATETIME(),
                    updated_at_utc = SYSUTCDATETIME()
                WHERE source_name = 'harvest_time_entries'
            """)
            conn.commit()
            cursor.close()
            conn.close()
            return

        # Clear stage
        logging.error("Step 5: truncating stage table")
        cursor.execute("TRUNCATE TABLE raw.harvest_time_entries_stage")
        conn.commit()

        # Insert to stage in chunks
        insert_sql = """
            INSERT INTO raw.harvest_time_entries_stage (
                id, spent_date, hours, rounded_hours, notes, is_billed, billable,
                project_id, project_name, task_id, task_name,
                user_id, user_name, client_id, client_name,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        chunk_size = 1000
        for i in range(0, len(all_rows), chunk_size):
            chunk = all_rows[i:i + chunk_size]
            logging.error(f"Inserting chunk {i} to {i + len(chunk)}")
            cursor.executemany(insert_sql, chunk)
            conn.commit()

        logging.error("Step 6 OK: stage load complete")

        # Merge stage into raw and update sync state
        cursor.execute("EXEC rpt.usp_merge_harvest_time_entries")
        conn.commit()

        cursor.execute("""
            UPDATE rpt.sync_state
            SET last_successful_sync_utc = SYSUTCDATETIME(),
                updated_at_utc = SYSUTCDATETIME()
            WHERE source_name = 'harvest_time_entries'
        """)
        conn.commit()

        cursor.close()
        conn.close()

        logging.error("=== FUNCTION COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        logging.error("=== FUNCTION FAILED ===")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error(f"Error message: {str(e)}")
        logging.error(traceback.format_exc())
        raise
