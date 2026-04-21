import json
from datetime import datetime, timezone

from .xero_client import get_paged


def normalise_xero_date(value):
    if not value:
        return None

    # Xero /Date(...) format
    if isinstance(value, str) and value.startswith("/Date("):
        try:
            digits = "".join(ch for ch in value if ch.isdigit())
            millis = digits[:13]
            return datetime.fromtimestamp(int(millis) / 1000, tz=timezone.utc)
        except Exception:
            return None

    # ISO string
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    return value


def safe_decimal(value):
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def get_xero_connections(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT connection_name, tenant_id, tenant_name
        FROM dbo.xero_oauth_state
        ORDER BY tenant_name
    """)
    return cursor.fetchall()


def load_accounts_for_connection(connection_name: str, tenant_id: str, tenant_name: str):
    from .xero_client import get_one

    data = get_one("Accounts", connection_name=connection_name)
    accounts = data.get("Accounts", [])

    rows = []
    loaded_at = datetime.now(timezone.utc)

    for a in accounts:
        rows.append((
            tenant_id,
            tenant_name,
            a.get("AccountID"),
            a.get("Code"),
            a.get("Name"),
            a.get("Type"),
            a.get("Status"),
            a.get("Description"),
            a.get("BankAccountNumber"),
            a.get("TaxType"),
            a.get("Class"),
            1 if a.get("EnablePaymentsToAccount") else 0 if a.get("EnablePaymentsToAccount") is not None else None,
            1 if a.get("ShowInExpenseClaims") else 0 if a.get("ShowInExpenseClaims") is not None else None,
            a.get("ReportingCode"),
            a.get("ReportingCodeName"),
            normalise_xero_date(a.get("UpdatedDateUTC")),
            loaded_at,
        ))

    return rows


def write_accounts_stage(conn, rows):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.xero_accounts_stage")
    conn.commit()

    insert_sql = """
        INSERT INTO raw.xero_accounts_stage (
            tenant_id,
            tenant_name,
            account_id,
            code,
            name,
            type,
            status,
            description,
            bank_account_number,
            tax_type,
            class,
            enable_payments_to_account,
            show_in_expense_claims,
            reporting_code,
            reporting_code_name,
            updated_date_utc,
            source_loaded_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()


def merge_accounts(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC rpt.usp_merge_xero_accounts")
    conn.commit()


def load_invoices_for_connection(connection_name: str, tenant_id: str, tenant_name: str):
    invoices = get_paged("Invoices", connection_name=connection_name)

    rows = []
    loaded_at = datetime.now(timezone.utc)

    for i in invoices:
        contact = i.get("Contact") or {}

        invoice_date = normalise_xero_date(i.get("Date"))
        due_date = normalise_xero_date(i.get("DueDate"))
        updated_date_utc = normalise_xero_date(i.get("UpdatedDateUTC"))
        fully_paid_on_date_utc = normalise_xero_date(i.get("FullyPaidOnDate"))

        rows.append((
            tenant_id,
            tenant_name,
            i.get("InvoiceID"),
            i.get("InvoiceNumber"),
            i.get("Type"),
            contact.get("ContactID"),
            contact.get("Name"),
            i.get("Status"),
            i.get("LineAmountTypes"),
            i.get("SubTotal"),
            i.get("TotalTax"),
            i.get("Total"),
            i.get("AmountDue"),
            i.get("AmountPaid"),
            i.get("AmountCredited"),
            i.get("CurrencyCode"),
            invoice_date,
            due_date,
            updated_date_utc,
            fully_paid_on_date_utc,
            i.get("Reference"),
            loaded_at,
        ))

    return rows


def load_invoice_lines_for_connection(connection_name: str, tenant_id: str, tenant_name: str):
    invoices = get_paged("Invoices", connection_name=connection_name)

    rows = []
    loaded_at = datetime.now(timezone.utc)

    for i in invoices:
        contact = i.get("Contact") or {}

        invoice_id = i.get("InvoiceID")
        invoice_number = i.get("InvoiceNumber")
        invoice_type = i.get("Type")
        invoice_status = i.get("Status")
        invoice_date = normalise_xero_date(i.get("Date"))
        currency_code = i.get("CurrencyCode")
        contact_name = contact.get("Name")

        line_items = i.get("LineItems") or []

        for idx, line in enumerate(line_items, start=1):
            tracking = line.get("Tracking") or []
            line_item_id = line.get("LineItemID") or f"{invoice_id}_{idx}"

            rows.append((
                tenant_id,
                tenant_name,
                invoice_id,
                invoice_number,
                invoice_date,
                contact_name,
                invoice_type,
                invoice_status,
                currency_code,
                line_item_id,
                idx,
                line.get("Description"),
                safe_decimal(line.get("Quantity")),
                safe_decimal(line.get("UnitAmount")),
                safe_decimal(line.get("LineAmount")),
                line.get("AccountCode"),
                line.get("TaxType"),
                json.dumps(tracking) if tracking else None,
                loaded_at,
            ))

    return rows


def write_invoices_stage(conn, rows):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.xero_invoices_stage")
    conn.commit()

    insert_sql = """
        INSERT INTO raw.xero_invoices_stage (
            tenant_id,
            tenant_name,
            invoice_id,
            invoice_number,
            invoice_type,
            contact_id,
            contact_name,
            invoice_status,
            line_amount_types,
            sub_total,
            total_tax,
            total,
            amount_due,
            amount_paid,
            amount_credited,
            currency_code,
            date_utc,
            due_date_utc,
            updated_date_utc,
            fully_paid_on_date_utc,
            reference,
            source_loaded_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()


def write_invoice_lines_stage(conn, rows):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.xero_invoice_lines_stage")
    conn.commit()

    insert_sql = """
        INSERT INTO raw.xero_invoice_lines_stage (
            tenant_id,
            tenant_name,
            invoice_id,
            invoice_number,
            invoice_date,
            contact_name,
            invoice_type,
            invoice_status,
            currency_code,
            line_item_id,
            line_num,
            description,
            quantity,
            unit_amount,
            line_amount,
            account_code,
            tax_type,
            tracking_json,
            source_loaded_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()


def merge_invoices(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC rpt.usp_merge_xero_invoices")
    conn.commit()


def merge_invoice_lines(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC rpt.usp_merge_xero_invoice_lines")
    conn.commit()


def load_payments_for_connection(connection_name: str, tenant_id: str, tenant_name: str):
    payments = get_paged("Payments", connection_name=connection_name)

    rows = []
    loaded_at = datetime.now(timezone.utc)

    for p in payments:
        invoice = p.get("Invoice") or {}
        account = p.get("Account") or {}

        payment_date = normalise_xero_date(p.get("Date"))

        rows.append((
            tenant_id,
            tenant_name,
            p.get("PaymentID"),
            invoice.get("InvoiceID"),
            invoice.get("InvoiceNumber"),
            account.get("AccountID"),
            account.get("Code"),
            account.get("Name"),
            p.get("PaymentType"),
            p.get("Status"),
            payment_date,
            p.get("Amount"),
            p.get("CurrencyRate"),
            p.get("Reference"),
            loaded_at,
        ))

    return rows


def write_payments_stage(conn, rows):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.xero_payments_stage")
    conn.commit()

    insert_sql = """
        INSERT INTO raw.xero_payments_stage (
            tenant_id,
            tenant_name,
            payment_id,
            invoice_id,
            invoice_number,
            account_id,
            account_code,
            account_name,
            payment_type,
            payment_status,
            date_utc,
            amount,
            currency_rate,
            reference,
            source_loaded_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()


def merge_payments(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC rpt.usp_merge_xero_payments")
    conn.commit()


def load_bank_transactions_for_connection(connection_name: str, tenant_id: str, tenant_name: str):
    bank_transactions = get_paged("BankTransactions", connection_name=connection_name)

    rows = []
    loaded_at = datetime.now(timezone.utc)

    for b in bank_transactions:
        contact = b.get("Contact") or {}
        bank_account = b.get("BankAccount") or {}

        rows.append((
            tenant_id,
            tenant_name,
            b.get("BankTransactionID"),
            b.get("Type"),
            contact.get("ContactID"),
            contact.get("Name"),
            bank_account.get("AccountID"),
            bank_account.get("Code"),
            bank_account.get("Name"),
            b.get("SubTotal"),
            b.get("TotalTax"),
            b.get("Total"),
            b.get("CurrencyCode"),
            b.get("Status"),
            normalise_xero_date(b.get("Date")),
            normalise_xero_date(b.get("UpdatedDateUTC")),
            b.get("Reference"),
            loaded_at,
        ))

    return rows


def write_bank_transactions_stage(conn, rows):
    if not rows:
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.xero_bank_transactions_stage")
    conn.commit()

    insert_sql = """
        INSERT INTO raw.xero_bank_transactions_stage (
            tenant_id,
            tenant_name,
            bank_transaction_id,
            bank_transaction_type,
            contact_id,
            contact_name,
            bank_account_id,
            bank_account_code,
            bank_account_name,
            sub_total,
            total_tax,
            total,
            currency_code,
            status,
            date_utc,
            updated_date_utc,
            reference,
            source_loaded_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()


def merge_bank_transactions(conn):
    cursor = conn.cursor()
    cursor.execute("EXEC rpt.usp_merge_xero_bank_transactions")
    conn.commit()
