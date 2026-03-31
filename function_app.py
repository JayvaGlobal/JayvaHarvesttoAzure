import os
import logging
import traceback
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def harvest_time_entries_incremental(mytimer: func.TimerRequest) -> None:
    logging.info("=== Function started ===")

    try:
        logging.info("Step 1: importing modules")
        import requests
        import pandas as pd
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import NullPool
        from urllib.parse import quote_plus
        logging.info("Step 1 OK: modules imported")

        logging.info("Step 2: clearing proxy env vars")
        for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"]:
            os.environ.pop(key, None)
        logging.info("Step 2 OK: proxy env vars cleared")

        logging.info("Step 3: reading environment variables")
        server = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        username = os.getenv("AZURE_SQL_USERNAME")
        password = os.getenv("AZURE_SQL_PASSWORD")
        harvest_account_id = os.getenv("HARVEST_ACCOUNT_ID")
        harvest_token = os.getenv("HARVEST_TOKEN")

        logging.info(f"AZURE_SQL_SERVER present: {bool(server)}")
        logging.info(f"AZURE_SQL_DATABASE present: {bool(database)}")
        logging.info(f"AZURE_SQL_USERNAME present: {bool(username)}")
        logging.info(f"AZURE_SQL_PASSWORD present: {bool(password)}")
        logging.info(f"HARVEST_ACCOUNT_ID present: {bool(harvest_account_id)}")
        logging.info(f"HARVEST_TOKEN present: {bool(harvest_token)}")

        if not all([server, database, username, password, harvest_account_id, harvest_token]):
            raise ValueError("One or more required environment variables are missing.")

        logging.info("Step 4: building SQL connection string")
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=60;"
        )
        logging.info("Step 4 OK: connection string built")

        logging.info("Step 5: creating SQLAlchemy engine")
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}",
            pool_pre_ping=True,
            pool_recycle=1800,
            poolclass=NullPool
        )
        logging.info("Step 5 OK: engine created")

        logging.info("Step 6: testing DB connection")
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Step 6 OK: DB connection successful")

        logging.info("Step 7: reading last sync time")
        with engine.begin() as conn:
            last_sync = conn.execute(text("""
                SELECT last_successful_sync_utc
                FROM rpt.sync_state
                WHERE source_name = 'harvest_time_entries'
            """)).scalar()
        logging.info(f"Step 7 OK: last_sync = {last_sync}")

        logging.info("Step 8: preparing Harvest request")
        headers = {
            "Authorization": f"Bearer {harvest_token}",
            "Harvest-Account-ID": harvest_account_id,
            "User-Agent": "jayva-harvest-function"
        }

        session = requests.Session()
        session.trust_env = False
        session.proxies = {}
        logging.info("Step 8 OK: session ready")

        logging.info("Step 9: requesting Harvest time entries")
        response = session.get(
            "https://api.harvestapp.com/v2/time_entries",
            headers=headers,
            params={
                "updated_since": last_sync.isoformat() if last_sync else "2025-01-01T00:00:00Z",
                "page": 1,
                "per_page": 10
            },
            timeout=60
        )
        logging.info(f"Harvest response status: {response.status_code}")
        response.raise_for_status()

        payload = response.json()
        entries = payload.get("time_entries", [])
        logging.info(f"Step 9 OK: pulled {len(entries)} entries from Harvest")

        logging.info("=== Function completed initial debug successfully ===")

    except Exception as e:
        logging.error("=== FUNCTION FAILED ===")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error(f"Error message: {str(e)}")
        logging.error(traceback.format_exc())
        raise
