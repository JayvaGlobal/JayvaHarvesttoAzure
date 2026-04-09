from auth import refresh_access_token, get_connections
from xero_client import get_paged
from loaders import (
    flatten_contacts, flatten_accounts, flatten_invoices,
    flatten_payments, flatten_credit_notes, flatten_bills
)
from db import get_engine, write_dataframe, run_reporting_refresh

def main():
    engine = get_engine()

    token_data = refresh_access_token()
    access_token = token_data["access_token"]

    connections = get_connections(access_token)

    for conn in connections:
        tenant_id = conn["tenantId"]
        tenant_name = conn["tenantName"]

        contacts = get_paged("Contacts", access_token, tenant_id)
        accounts = get_paged("Accounts", access_token, tenant_id)
        invoices = get_paged("Invoices", access_token, tenant_id)
        payments = get_paged("Payments", access_token, tenant_id)
        credit_notes = get_paged("CreditNotes", access_token, tenant_id)

        contacts_df = flatten_contacts(contacts, tenant_name)
        accounts_df = flatten_accounts(accounts, tenant_name)
        invoice_df, invoice_lines_df = flatten_invoices(invoices, tenant_name)
        payments_df = flatten_payments(payments, tenant_name)
        credit_notes_df = flatten_credit_notes(credit_notes, tenant_name)

        bills_source = [x for x in invoices if x.get("Type") == "ACCPAY"]
        bills_df, bill_lines_df = flatten_bills(bills_source, tenant_name)

        # easiest first pass: delete tenant rows and reload
        with engine.begin() as conn_sql:
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.contacts WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.accounts WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.invoices WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.invoice_line_items WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.payments WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.credit_notes WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.bills WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")
            conn_sql.exec_driver_sql(f"DELETE FROM stg_xero.bill_line_items WHERE tenant_name = '{tenant_name.replace(\"'\",\"''\")}'")

        write_dataframe(contacts_df, "contacts", engine)
        write_dataframe(accounts_df, "accounts", engine)
        write_dataframe(invoice_df, "invoices", engine)
        write_dataframe(invoice_lines_df, "invoice_line_items", engine)
        write_dataframe(payments_df, "payments", engine)
        write_dataframe(credit_notes_df, "credit_notes", engine)
        write_dataframe(bills_df, "bills", engine)
        write_dataframe(bill_lines_df, "bill_line_items", engine)

    run_reporting_refresh(engine)

if __name__ == "__main__":
    main()
