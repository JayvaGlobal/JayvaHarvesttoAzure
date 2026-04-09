from sqlalchemy import create_engine, text
import pandas as pd
from config import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

def get_engine():
    conn_str = (
        f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={SQL_DRIVER.replace(' ', '+')}"
    )
    return create_engine(conn_str, fast_executemany=True)

def get_watermark(engine, tenant_name: str, entity_type: str):
    sql = text("""
        SELECT last_watermark_utc
        FROM dbo.xero_etl_state
        WHERE tenant_name = :tenant_name
          AND entity_type = :entity_type
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"tenant_name": tenant_name, "entity_type": entity_type}).fetchone()
    return row[0] if row else None

def upsert_state(engine, tenant_name: str, entity_type: str, watermark, row_count: int, status: str):
    sql = text("""
        MERGE dbo.xero_etl_state AS tgt
        USING (
            SELECT :tenant_name AS tenant_name,
                   :entity_type AS entity_type
        ) AS src
        ON tgt.tenant_name = src.tenant_name
       AND tgt.entity_type = src.entity_type
        WHEN MATCHED THEN UPDATE SET
            last_watermark_utc = :watermark,
            last_successful_run_utc = SYSUTCDATETIME(),
            last_row_count = :row_count,
            status = :status,
            updated_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (tenant_name, entity_type, last_watermark_utc, last_successful_run_utc, last_row_count, status, updated_at_utc)
            VALUES (:tenant_name, :entity_type, :watermark, SYSUTCDATETIME(), :row_count, :status, SYSUTCDATETIME());
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "tenant_name": tenant_name,
            "entity_type": entity_type,
            "watermark": watermark,
            "row_count": row_count,
            "status": status
        })

def write_dataframe(df: pd.DataFrame, table_name: str, engine, schema: str = "stg_xero", if_exists: str = "append"):
    if df.empty:
        return
    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False, chunksize=1000, method="multi")

def run_reporting_refresh(engine):
    with engine.begin() as conn:
        conn.execute(text("EXEC rpt.sp_refresh_reporting_layer"))
