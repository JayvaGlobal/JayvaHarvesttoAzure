from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text
from .config import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER


def get_engine():
    conn_str = (
        f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={SQL_DRIVER.replace(' ', '+')}"
    )
    return create_engine(conn_str, fast_executemany=True)


def write_dataframe(df, table_name, engine, schema="stg_xero", if_exists="append"):
    if df.empty:
        print(f"No rows to write for {schema}.{table_name}")
        return

    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=1000,
        method="multi",
    )


def ensure_xero_oauth_table(engine):
    sql = """
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables
        WHERE name = 'xero_oauth_state'
          AND schema_id = SCHEMA_ID('dbo')
    )
    BEGIN
        CREATE TABLE dbo.xero_oauth_state (
            connection_name NVARCHAR(100) NOT NULL PRIMARY KEY,
            tenant_id NVARCHAR(100) NULL,
            tenant_name NVARCHAR(255) NULL,
            access_token NVARCHAR(MAX) NULL,
            refresh_token NVARCHAR(MAX) NOT NULL,
            access_token_expires_at DATETIME2 NULL,
            refresh_token_updated_at DATETIME2 NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def save_xero_connection(engine, connection_name, tenant_id, tenant_name, access_token, refresh_token, expires_in):
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in) - 120)

    sql = text("""
    MERGE dbo.xero_oauth_state AS target
    USING (
        SELECT
            :connection_name AS connection_name,
            :tenant_id AS tenant_id,
            :tenant_name AS tenant_name,
            :access_token AS access_token,
            :refresh_token AS refresh_token,
            :access_token_expires_at AS access_token_expires_at
    ) AS src
    ON target.connection_name = src.connection_name
    WHEN MATCHED THEN
        UPDATE SET
            tenant_id = src.tenant_id,
            tenant_name = src.tenant_name,
            access_token = src.access_token,
            refresh_token = src.refresh_token,
            access_token_expires_at = src.access_token_expires_at,
            refresh_token_updated_at = SYSUTCDATETIME(),
            updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (
            connection_name,
            tenant_id,
            tenant_name,
            access_token,
            refresh_token,
            access_token_expires_at,
            refresh_token_updated_at,
            updated_at
        )
        VALUES (
            src.connection_name,
            src.tenant_id,
            src.tenant_name,
            src.access_token,
            src.refresh_token,
            src.access_token_expires_at,
            SYSUTCDATETIME(),
            SYSUTCDATETIME()
        );
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "connection_name": connection_name,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "access_token_expires_at": expires_at,
        })


def load_xero_connection(engine, connection_name):
    sql = text("""
        SELECT
            connection_name,
            tenant_id,
            tenant_name,
            access_token,
            refresh_token,
            access_token_expires_at
        FROM dbo.xero_oauth_state
        WHERE connection_name = :connection_name
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {"connection_name": connection_name}).mappings().first()

    if not row:
        raise Exception(f"No Xero token row found for {connection_name}")

    return dict(row)


def update_xero_tokens(engine, connection_name, access_token, refresh_token, expires_in):
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in) - 120)

    sql = text("""
        UPDATE dbo.xero_oauth_state
        SET access_token = :access_token,
            refresh_token = :refresh_token,
            access_token_expires_at = :access_token_expires_at,
            refresh_token_updated_at = SYSUTCDATETIME(),
            updated_at = SYSUTCDATETIME()
        WHERE connection_name = :connection_name
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "connection_name": connection_name,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "access_token_expires_at": expires_at,
        })
