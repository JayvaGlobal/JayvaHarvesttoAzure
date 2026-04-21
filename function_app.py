import os
import logging
import traceback
import azure.functions as func

app = func.FunctionApp()


def update_sync_state(conn, source_name: str) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        MERGE rpt.sync_state AS tgt
        USING (SELECT ? AS source_name) AS src
        ON tgt.source_name = src.source_name
        WHEN MATCHED THEN
            UPDATE SET
                last_successful_sync_utc = SYSUTCDATETIME(),
                updated_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (source_name, last_successful_sync_utc, updated_at_utc)
            VALUES (src.source_name, SYSUTCDATETIME(), SYSUTCDATETIME());
    """, (source_name,))
    conn.commit()
    cursor.close()


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def harvest_time_entries_incremental(mytimer: func.TimerRequest) -> None:
    logging.error("=== FUNCTION STARTED ===")

    try:
        import requests
        import pandas as pd
        from mssql_python import connect

        logging.error("Step 1 OK: modules imported")

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

        cursor.execute("SELECT 1")
        cursor.fetchone()
        logging.error("Step 2 OK: DB connection successful")

        cursor.execute("""
            SELECT last_successful_sync_utc
            FROM rpt.sync_state
            WHERE source_name = 'harvest_time_entries'
        """)
        row = cursor.fetchone()
        last_sync = row[0] if row else None
        logging.error(f"Step 3 OK: last_sync = {last_sync}")

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

        logging.error("Step 5: truncating stage table")
        cursor.execute("TRUNCATE TABLE raw.harvest_time_entries_stage")
        conn.commit()

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


@app.route(route="ping", auth_level=func.AuthLevel.ANONYMOUS)
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("pong", status_code=200)


@app.route(route="xero_callback", auth_level=func.AuthLevel.ANONYMOUS)
def xero_callback(req: func.HttpRequest) -> func.HttpResponse:
    logging.error("=== XERO CALLBACK STARTED ===")

    try:
        code = req.params.get("code")

        if not code:
            return func.HttpResponse("Missing code", status_code=400)

        from xero.auth import save_initial_tokens_from_code

        saved = save_initial_tokens_from_code(code)

        lines = ["Saved Xero connections:"]
        for s in saved:
            lines.append(f"{s['connection_name']} - {s['tenant_name']} ({s['tenant_id']})")

        logging.error("=== XERO CALLBACK COMPLETED SUCCESSFULLY ===")
        return func.HttpResponse("\n".join(lines), status_code=200)

    except Exception as e:
        logging.error("=== XERO CALLBACK FAILED ===")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error(f"Error message: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="xero_test", auth_level=func.AuthLevel.ANONYMOUS)
def xero_test(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.xero_client import get_one

        connection_name = req.params.get("connection_name")
        if not connection_name:
            return func.HttpResponse("Missing connection_name", status_code=400)

        data = get_one("Organisation", connection_name=connection_name)
        return func.HttpResponse(str(data), status_code=200)

    except Exception as e:
        logging.error(f"Xero test failed: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_token_keepalive(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO TOKEN KEEPALIVE STARTED ===")

    try:
        from xero.auth import get_connection, get_valid_access_token

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT connection_name FROM dbo.xero_oauth_state")
        rows = cursor.fetchall()

        for row in rows:
            connection_name = row[0]
            try:
                get_valid_access_token(connection_name)
                logging.error(f"Token OK for {connection_name}")
            except Exception as inner_e:
                logging.error(f"Token refresh failed for {connection_name}: {str(inner_e)}")

        update_sync_state(conn, "xero_token_keepalive")

        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Xero keepalive failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise

    logging.error("=== XERO TOKEN KEEPALIVE COMPLETED ===")


@app.route(route="xero_contacts_import", auth_level=func.AuthLevel.ANONYMOUS)
def xero_contacts_import(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_contacts_for_connection,
            write_contacts_stage,
            merge_contacts,
        )

        connection_name = req.params.get("connection_name")
        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if connection_name:
                all_conn_rows = [r for r in all_conn_rows if r[0] == connection_name]

            if not all_conn_rows:
                return func.HttpResponse("No matching Xero connection found", status_code=404)

            all_rows = []

            for row in all_conn_rows:
                conn_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading contacts for {tenant_name} ({conn_name})")
                rows = load_contacts_for_connection(conn_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_contacts_stage(conn, all_rows)
            merge_contacts(conn)

            return func.HttpResponse(
                f"Loaded {len(all_rows)} contact rows for {len(all_conn_rows)} tenant(s)",
                status_code=200
            )

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero contacts import failed: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="xero_accounts_import", auth_level=func.AuthLevel.ANONYMOUS)
def xero_accounts_import(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_accounts_for_connection,
            write_accounts_stage,
            merge_accounts,
        )

        connection_name = req.params.get("connection_name")
        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)
            if connection_name:
                all_conn_rows = [r for r in all_conn_rows if r[0] == connection_name]

            if not all_conn_rows:
                return func.HttpResponse("No matching Xero connection found", status_code=404)

            all_rows = []
            for row in all_conn_rows:
                rows = load_accounts_for_connection(row[0], row[1], row[2])
                all_rows.extend(rows)

            write_accounts_stage(conn, all_rows)
            merge_accounts(conn)

            return func.HttpResponse(f"Loaded {len(all_rows)} account rows", status_code=200)

        finally:
            conn.close()

    except Exception as e:
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="xero_invoices_import", auth_level=func.AuthLevel.ANONYMOUS)
def xero_invoices_import(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_invoices_for_connection,
            write_invoices_stage,
            merge_invoices,
        )

        connection_name = req.params.get("connection_name")
        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)
            if connection_name:
                all_conn_rows = [r for r in all_conn_rows if r[0] == connection_name]

            if not all_conn_rows:
                return func.HttpResponse("No matching Xero connection found", status_code=404)

            all_rows = []
            for row in all_conn_rows:
                rows = load_invoices_for_connection(row[0], row[1], row[2])
                all_rows.extend(rows)

            write_invoices_stage(conn, all_rows)
            merge_invoices(conn)

            return func.HttpResponse(f"Loaded {len(all_rows)} invoice rows", status_code=200)

        finally:
            conn.close()

    except Exception as e:
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="xero_payments_import", auth_level=func.AuthLevel.ANONYMOUS)
def xero_payments_import(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_payments_for_connection,
            write_payments_stage,
            merge_payments,
        )

        connection_name = req.params.get("connection_name")
        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)
            if connection_name:
                all_conn_rows = [r for r in all_conn_rows if r[0] == connection_name]

            if not all_conn_rows:
                return func.HttpResponse("No matching Xero connection found", status_code=404)

            all_rows = []
            for row in all_conn_rows:
                rows = load_payments_for_connection(row[0], row[1], row[2])
                all_rows.extend(rows)

            write_payments_stage(conn, all_rows)
            merge_payments(conn)

            return func.HttpResponse(f"Loaded {len(all_rows)} payment rows", status_code=200)

        finally:
            conn.close()

    except Exception as e:
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="xero_bank_transactions_import", auth_level=func.AuthLevel.ANONYMOUS)
def xero_bank_transactions_import(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_bank_transactions_for_connection,
            write_bank_transactions_stage,
            merge_bank_transactions,
        )

        connection_name = req.params.get("connection_name")
        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)
            if connection_name:
                all_conn_rows = [r for r in all_conn_rows if r[0] == connection_name]

            if not all_conn_rows:
                return func.HttpResponse("No matching Xero connection found", status_code=404)

            all_rows = []
            for row in all_conn_rows:
                rows = load_bank_transactions_for_connection(row[0], row[1], row[2])
                all_rows.extend(rows)

            write_bank_transactions_stage(conn, all_rows)
            merge_bank_transactions(conn)

            return func.HttpResponse(f"Loaded {len(all_rows)} bank transaction rows", status_code=200)

        finally:
            conn.close()

    except Exception as e:
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="fx_rates_gbp_import", auth_level=func.AuthLevel.ANONYMOUS)
def fx_rates_gbp_import(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.fx_rates import (
            get_latest_gbp_rates,
            normalise_latest_payload_to_rows,
            upsert_fx_rates,
        )

        payload = get_latest_gbp_rates()
        rows = normalise_latest_payload_to_rows(payload)
        upsert_fx_rates(rows)

        return func.HttpResponse(
            f"Loaded {len(rows)} GBP FX rows for {payload.get('date')}",
            status_code=200
        )

    except Exception as e:
        logging.error(f"GBP FX import failed: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="fx_rates_gbp_backfill", auth_level=func.AuthLevel.ANONYMOUS)
def fx_rates_gbp_backfill(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from xero.fx_rates import (
            get_historical_gbp_rates,
            normalise_historical_payload_to_rows,
            upsert_fx_rates,
        )

        start_date = req.params.get("start_date")
        end_date = req.params.get("end_date")

        if not start_date or not end_date:
            return func.HttpResponse("Missing start_date or end_date", status_code=400)

        payload = get_historical_gbp_rates(start_date, end_date)
        rows = normalise_historical_payload_to_rows(payload)
        upsert_fx_rates(rows)

        return func.HttpResponse(
            f"Loaded {len(rows)} GBP FX rows from {start_date} to {end_date}",
            status_code=200
        )

    except Exception as e:
        logging.error(f"GBP FX backfill failed: {str(e)}")
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_invoices_import_daily(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO INVOICES IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_invoices_for_connection,
            write_invoices_stage,
            merge_invoices,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for invoice import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading invoices for {tenant_name} ({connection_name})")
                rows = load_invoices_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_invoices_stage(conn, all_rows)
            merge_invoices(conn)
            update_sync_state(conn, "xero_invoices")

            logging.error(f"Xero invoices import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero invoices import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_payments_import_daily(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO PAYMENTS IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_payments_for_connection,
            write_payments_stage,
            merge_payments,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for payments import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading payments for {tenant_name} ({connection_name})")
                rows = load_payments_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_payments_stage(conn, all_rows)
            merge_payments(conn)
            update_sync_state(conn, "xero_payments")

            logging.error(f"Xero payments import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero payments import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_accounts_import_daily(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO ACCOUNTS IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_accounts_for_connection,
            write_accounts_stage,
            merge_accounts,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for accounts import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading accounts for {tenant_name} ({connection_name})")
                rows = load_accounts_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_accounts_stage(conn, all_rows)
            merge_accounts(conn)
            update_sync_state(conn, "xero_accounts")

            logging.error(f"Xero accounts import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero accounts import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_contacts_import_daily(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO CONTACTS IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_contacts_for_connection,
            write_contacts_stage,
            merge_contacts,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for contacts import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading contacts for {tenant_name} ({connection_name})")
                rows = load_contacts_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_contacts_stage(conn, all_rows)
            merge_contacts(conn)
            update_sync_state(conn, "xero_contacts")

            logging.error(f"Xero contacts import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero contacts import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_bank_transactions_import_daily(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO BANK TRANSACTIONS IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_bank_transactions_for_connection,
            write_bank_transactions_stage,
            merge_bank_transactions,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for bank transactions import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading bank transactions for {tenant_name} ({connection_name})")
                rows = load_bank_transactions_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_bank_transactions_stage(conn, all_rows)
            merge_bank_transactions(conn)
            update_sync_state(conn, "xero_bank_transactions")

            logging.error(f"Xero bank transactions import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero bank transactions import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def fx_rates_gbp_daily(mytimer: func.TimerRequest) -> None:
    logging.error("=== GBP FX IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.fx_rates import (
            get_latest_gbp_rates,
            normalise_latest_payload_to_rows,
            upsert_fx_rates,
        )

        payload = get_latest_gbp_rates()
        rows = normalise_latest_payload_to_rows(payload)
        upsert_fx_rates(rows)

        conn = get_connection()
        try:
            update_sync_state(conn, "fx_rates_gbp_daily")
        finally:
            conn.close()

        logging.error(f"GBP FX import complete. Rows loaded: {len(rows)}")

    except Exception as e:
        logging.error(f"GBP FX import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise

    logging.error("=== GBP FX IMPORT COMPLETED ===")
