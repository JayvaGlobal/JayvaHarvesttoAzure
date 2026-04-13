from datetime import datetime, timedelta, timezone
from mssql_python import connect

from .config import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD


def get_connection():
    sql_connection_string = (
        f"Server={SQL_SERVER};"
        f"Database={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Authentication=SqlPassword;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    return connect(sql_connection_string)


def ensure_xero_oauth_table(conn):
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
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()


def save_xero_connection(conn, connection_name, tenant_id, tenant_name, access_token, refresh_token, expires_in):
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in) - 120)

    sql = """
    MERGE dbo.xero_oauth_state AS target
    USING (
        SELECT
            ? AS connection_name,
            ? AS tenant_id,
            ? AS tenant_name,
            ? AS access_token,
            ? AS refresh_token,
            ? AS access_token_expires_at
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
    """
    cursor = conn.cursor()
    cursor.execute(
        sql,
        (
            connection_name,
            tenant_id,
            tenant_name,
            access_token,
            refresh_token,
            expires_at,
        ),
    )
    conn.commit()


def load_xero_connection(conn, connection_name):
    sql = """
        SELECT
            connection_name,
            tenant_id,
            tenant_name,
            access_token,
            refresh_token,
            access_token_expires_at
        FROM dbo.xero_oauth_state
        WHERE connection_name = ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (connection_name,))
    row = cursor.fetchone()

    if not row:
        raise Exception(f"No Xero token row found for {connection_name}")

    return {
        "connection_name": row[0],
        "tenant_id": row[1],
        "tenant_name": row[2],
        "access_token": row[3],
        "refresh_token": row[4],
        "access_token_expires_at": row[5],
    }


def update_xero_tokens(conn, connection_name, access_token, refresh_token, expires_in):
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in) - 120)

    sql = """
        UPDATE dbo.xero_oauth_state
        SET access_token = ?,
            refresh_token = ?,
            access_token_expires_at = ?,
            refresh_token_updated_at = SYSUTCDATETIME(),
            updated_at = SYSUTCDATETIME()
        WHERE connection_name = ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (access_token, refresh_token, expires_at, connection_name))
    conn.commit()
