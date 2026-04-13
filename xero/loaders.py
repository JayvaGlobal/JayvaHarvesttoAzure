from datetime import datetime, timezone
from mssql_python import connect

from .config import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
from .db import get_connection
from .xero_client import get_paged


def normalise_xero_date(value):
    if not value:
        return None

    if isinstance(value, str) and value.startswith("/Date("):
        try:
            ms = int(value.replace("/Date(", "").replace(")/", ""))
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        except Exception:
            return None

    return value


def load_contacts_for_connection(connection_name: str, tenant_id: str, tenant_name: str):
    contacts = get_paged("Contacts", connection_name=connection_name)

    rows = []
    loaded_at = datetime.now(timezone.utc)

    for c in contacts:
        rows.append((
            tenant_id,
            tenant_name,
            c.get("ContactID"),
            c.get("ContactNumber"),
            c.get("Name"),
            c.get("FirstName"),
            c.get("LastName"),
            c.get("EmailAddress"),
            1 if c.get("IsCustomer") else 0 if c.get("IsCustomer") is not None else None,
            1 if c.get("IsSupplier") else 0 if c.get("IsSupplier") is not None else None,
            c.get("ContactStatus"),
            normalise_xero_date(c.get("UpdatedDateUTC")),
            loaded_at,
        ))

    return rows


def write_contacts_stage(conn, rows):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.xero_contacts_stage")
    conn.commit()

    insert_sql = """
        INSERT INTO raw.xero_contacts_stage (
            tenant_id,
            tenant_name,
            contact_id,
            contact_number,
            name,
            first_name,
            last_name,
            email_address,
            is_customer,
            is_supplier,
            contact_status,
            updated_date_utc,
            source_loaded_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()


def merge_contacts(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC rpt.usp_merge_xero_contacts")
    conn.commit()


def get_xero_connections(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT connection_name, tenant_id, tenant_name
        FROM dbo.xero_oauth_state
        ORDER BY tenant_name
    """)
    return cursor.fetchall()
