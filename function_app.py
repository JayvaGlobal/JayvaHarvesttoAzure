import os
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def harvest_time_entries_incremental(mytimer: func.TimerRequest) -> None:
    import requests
    import pandas as pd
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool
    from urllib.parse import quote_plus

    for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"]:
        os.environ.pop(key, None)

    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")

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

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}",
        pool_pre_ping=True,
        pool_recycle=1800,
        poolclass=NullPool
    )

    headers = {
        "Authorization": f"Bearer {os.getenv('HARVEST_TOKEN')}",
        "Harvest-Account-ID": os.getenv("HARVEST_ACCOUNT_ID"),
        "User-Agent": "jayva-harvest-function"
    }

    session = requests.Session()
    session.trust_env = False
    session.proxies = {}

    with engine.begin() as conn:
        last_sync = conn.execute(text("""
            SELECT last_successful_sync_utc
            FROM rpt.sync_state
            WHERE source_name = 'harvest_time_entries'
        """)).scalar()

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

            all_rows.append({
                "id": e.get("id"),
                "spent_date": e.get("spent_date"),
                "hours": e.get("hours"),
                "rounded_hours": e.get("rounded_hours"),
                "notes": e.get("notes"),
                "is_billed": e.get("is_billed"),
                "billable": e.get("billable"),
                "project_id": project.get("id"),
                "project_name": project.get("name"),
                "task_id": task.get("id"),
                "task_name": task.get("name"),
                "user_id": user.get("id"),
                "user_name": user.get("name"),
                "client_id": client.get("id"),
                "client_name": client.get("name"),
                "created_at": e.get("created_at"),
                "updated_at": e.get("updated_at"),
            })

        total_pages = payload.get("total_pages", page)
        if page >= total_pages:
            break
        page += 1

    if not all_rows:
        return

    df = pd.DataFrame(all_rows)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.harvest_time_entries_stage"))

    df.to_sql(
        "harvest_time_entries_stage",
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=1000
    )

    with engine.begin() as conn:
        conn.execute(text("EXEC rpt.usp_merge_harvest_time_entries"))
        conn.execute(text("""
            UPDATE rpt.sync_state
            SET last_successful_sync_utc = SYSUTCDATETIME(),
                updated_at_utc = SYSUTCDATETIME()
            WHERE source_name = 'harvest_time_entries'
        """))
