import pandas as pd

def flatten_contacts(rows, tenant_name):
    out = []
    for r in rows:
        out.append({
            "contact_id": r.get("ContactID"),
            "contact_name": r.get("Name"),
            "contact_status": r.get("ContactStatus"),
            "email_address": r.get("EmailAddress"),
            "is_customer": 1 if r.get("IsCustomer") else 0,
            "is_supplier": 1 if r.get("IsSupplier") else 0,
            "updated_date_utc": r.get("UpdatedDateUTC"),
            "tenant_name": tenant_name,
        })
    return pd.DataFrame(out)

def flatten_accounts(rows, tenant_name):
    out = []
    for r in rows:
        out.append({
            "account_id": r.get("AccountID"),
            "code": r.get("Code"),
            "name": r.get("Name"),
            "type": r.get("Type"),
            "status": r.get("Status"),
            "description": r.get("Description"),
            "bank_account_number": r.get("BankAccountNumber"),
            "tax_type": r.get("TaxType"),
            "enable_payments_to_account": 1 if r.get("EnablePaymentsToAccount") else 0,
            "updated_date_utc": r.get("UpdatedDateUTC"),
            "tenant_name": tenant_name,
        })
    return pd.DataFrame(out)

def flatten_invoices(rows, tenant_name):
    header_rows = []
    line_rows = []
    for r in rows:
        contact = r.get("Contact", {}) or {}
        header_rows.append({
            "invoice_id": r.get("InvoiceID"),
            "invoice_number": r.get("InvoiceNumber"),
            "invoice_type": r.get("Type"),
            "contact_id": contact.get("ContactID"),
            "contact_name": contact.get("Name"),
            "status": r.get("Status"),
            "invoice_date": str(r.get("DateString") or "")[:10] if r.get("DateString") else None,
            "due_date": str(r.get("DueDateString") or "")[:10] if r.get("DueDateString") else None,
            "sub_total": r.get("SubTotal"),
            "total_tax": r.get("TotalTax"),
            "total": r.get("Total"),
            "amount_due": r.get("AmountDue"),
            "amount_paid": r.get("AmountPaid"),
            "amount_credited": r.get("AmountCredited"),
            "currency_code": r.get("CurrencyCode"),
            "reference": r.get("Reference"),
            "updated_date_utc": r.get("UpdatedDateUTC"),
            "tenant_name": tenant_name,
        })
        for idx, line in enumerate(r.get("LineItems", []) or [], start=1):
            tracking = line.get("Tracking", []) or []
            t1 = tracking[0] if len(tracking) > 0 else {}
            t2 = tracking[1] if len(tracking) > 1 else {}
            line_rows.append({
                "invoice_id": r.get("InvoiceID"),
                "line_num": idx,
                "description": line.get("Description"),
                "quantity": line.get("Quantity"),
                "unit_amount": line.get("UnitAmount"),
                "line_amount": line.get("LineAmount"),
                "account_code": line.get("AccountCode"),
                "tax_type": line.get("TaxType"),
                "tracking_1_name": t1.get("Name"),
                "tracking_1_option": t1.get("Option"),
                "tracking_2_name": t2.get("Name"),
                "tracking_2_option": t2.get("Option"),
                "tenant_name": tenant_name,
            })
    return pd.DataFrame(header_rows), pd.DataFrame(line_rows)

def flatten_payments(rows, tenant_name):
    out = []
    for r in rows:
        invoice = r.get("Invoice", {}) or {}
        out.append({
            "payment_id": r.get("PaymentID"),
            "invoice_id": invoice.get("InvoiceID"),
            "payment_date": str(r.get("Date") or "")[:10] if r.get("Date") else None,
            "amount": r.get("Amount"),
            "reference": r.get("Reference"),
            "payment_type": r.get("PaymentType"),
            "updated_date_utc": r.get("UpdatedDateUTC"),
            "tenant_name": tenant_name,
        })
    return pd.DataFrame(out)

def flatten_credit_notes(rows, tenant_name):
    out = []
    for r in rows:
        contact = r.get("Contact", {}) or {}
        out.append({
            "credit_note_id": r.get("CreditNoteID"),
            "credit_note_number": r.get("CreditNoteNumber"),
            "contact_id": contact.get("ContactID"),
            "contact_name": contact.get("Name"),
            "status": r.get("Status"),
            "credit_note_date": str(r.get("DateString") or "")[:10] if r.get("DateString") else None,
            "total": r.get("Total"),
            "remaining_credit": r.get("RemainingCredit"),
            "updated_date_utc": r.get("UpdatedDateUTC"),
            "tenant_name": tenant_name,
        })
    return pd.DataFrame(out)

def flatten_bills(rows, tenant_name):
    header_rows = []
    line_rows = []
    for r in rows:
        contact = r.get("Contact", {}) or {}
        header_rows.append({
            "bill_id": r.get("InvoiceID"),
            "bill_number": r.get("InvoiceNumber"),
            "contact_id": contact.get("ContactID"),
            "contact_name": contact.get("Name"),
            "status": r.get("Status"),
            "bill_date": str(r.get("DateString") or "")[:10] if r.get("DateString") else None,
            "due_date": str(r.get("DueDateString") or "")[:10] if r.get("DueDateString") else None,
            "sub_total": r.get("SubTotal"),
            "total_tax": r.get("TotalTax"),
            "total": r.get("Total"),
            "amount_due": r.get("AmountDue"),
            "amount_paid": r.get("AmountPaid"),
            "currency_code": r.get("CurrencyCode"),
            "reference": r.get("Reference"),
            "updated_date_utc": r.get("UpdatedDateUTC"),
            "tenant_name": tenant_name,
        })
        for idx, line in enumerate(r.get("LineItems", []) or [], start=1):
            tracking = line.get("Tracking", []) or []
            t1 = tracking[0] if len(tracking) > 0 else {}
            t2 = tracking[1] if len(tracking) > 1 else {}
            line_rows.append({
                "bill_id": r.get("InvoiceID"),
                "line_num": idx,
                "description": line.get("Description"),
                "quantity": line.get("Quantity"),
                "unit_amount": line.get("UnitAmount"),
                "line_amount": line.get("LineAmount"),
                "account_code": line.get("AccountCode"),
                "tax_type": line.get("TaxType"),
                "tracking_1_name": t1.get("Name"),
                "tracking_1_option": t1.get("Option"),
                "tracking_2_name": t2.get("Name"),
                "tracking_2_option": t2.get("Option"),
                "tenant_name": tenant_name,
            })
    return pd.DataFrame(header_rows), pd.DataFrame(line_rows)
