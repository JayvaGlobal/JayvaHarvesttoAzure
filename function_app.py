import os
import logging
import traceback
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def harvest_time_entries_incremental(mytimer: func.TimerRequest) -> None:
    logging.error("=== ENTERED FUNCTION ===")
    print("=== ENTERED FUNCTION ===", flush=True)

    try:
        logging.error("Step 1: importing requests")
        print("Step 1: importing requests", flush=True)
        import requests

        logging.error("Step 2: importing pandas")
        print("Step 2: importing pandas", flush=True)
        import pandas as pd

        logging.error("Step 3: importing sqlalchemy")
        print("Step 3: importing sqlalchemy", flush=True)
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import NullPool

        logging.error("Step 4: importing quote_plus")
        print("Step 4: importing quote_plus", flush=True)
        from urllib.parse import quote_plus

        logging.error("Step 5: imports completed")
        print("Step 5: imports completed", flush=True)

        logging.error("Step 6: reading environment variables")
        print("Step 6: reading environment variables", flush=True)
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

        print(f"AZURE_SQL_SERVER present: {bool(server)}", flush=True)
        print(f"AZURE_SQL_DATABASE present: {bool(database)}", flush=True)
        print(f"AZURE_SQL_USERNAME present: {bool(username)}", flush=True)
        print(f"AZURE_SQL_PASSWORD present: {bool(password)}", flush=True)
        print(f"HARVEST_ACCOUNT_ID present: {bool(harvest_account_id)}", flush=True)
        print(f"HARVEST_TOKEN present: {bool(harvest_token)}", flush=True)

        if not all([server, database, username, password, harvest_account_id, harvest_token]):
            raise ValueError("One or more required environment variables are missing.")

        logging.error("Step 7: building SQL connection string")
        print("Step 7: building SQL connection string", flush=True)
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

        logging.error("Step 8: creating SQLAlchemy engine")
        print("Step 8: creating SQLAlchemy engine", flush=True)
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}",
            pool_pre_ping=True,
            pool_recycle=1800,
            poolclass=NullPool
        )

        logging.error("Step 9: testing DB connection")
        print("Step 9: testing DB connection", flush=True)
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))

        logging.error("Step 10: DB connection successful")
        print("Step 10: DB connection successful", flush=True)

        logging.error("=== DEBUG CHECKPOINT COMPLETE ===")
        print("=== DEBUG CHECKPOINT COMPLETE ===", flush=True)

    except Exception as e:
        logging.error("=== FUNCTION FAILED ===")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error(f"Error message: {str(e)}")
        logging.error(traceback.format_exc())

        print("=== FUNCTION FAILED ===", flush=True)
        print(f"Error type: {type(e).__name__}", flush=True)
        print(f"Error message: {str(e)}", flush=True)
        print(traceback.format_exc(), flush=True)

        raise
