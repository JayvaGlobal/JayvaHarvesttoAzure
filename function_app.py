def update_sync_state(conn, source_name: str) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        MERGE rpt.sync_state AS tgt
        USING (SELECT ? AS source_name) AS src
        ON tgt.source_name = src.source_name
        WHEN MATCHED THEN
            UPDATE SET
                last_successful_sync_utc = SYSUTCDATETIME(),
                updated_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (source_name, last_successful_sync_utc, updated_at_utc)
            VALUES (src.source_name, SYSUTCDATETIME(), SYSUTCDATETIME());
    """, (source_name,))
    conn.commit()
    cursor.close()


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_invoices_import_timer(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO INVOICES TIMER IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_invoices_for_connection,
            write_invoices_stage,
            merge_invoices,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for invoice import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading invoices for {tenant_name} ({connection_name})")
                rows = load_invoices_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_invoices_stage(conn, all_rows)
            merge_invoices(conn)
            update_sync_state(conn, "xero_invoices")

            logging.error(f"Xero invoices timer import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero invoices timer import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="10 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_payments_import_timer(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO PAYMENTS TIMER IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_payments_for_connection,
            write_payments_stage,
            merge_payments,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for payments import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading payments for {tenant_name} ({connection_name})")
                rows = load_payments_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_payments_stage(conn, all_rows)
            merge_payments(conn)
            update_sync_state(conn, "xero_payments")

            logging.error(f"Xero payments timer import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero payments timer import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="20 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_accounts_import_timer(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO ACCOUNTS TIMER IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_accounts_for_connection,
            write_accounts_stage,
            merge_accounts,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for accounts import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading accounts for {tenant_name} ({connection_name})")
                rows = load_accounts_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_accounts_stage(conn, all_rows)
            merge_accounts(conn)
            update_sync_state(conn, "xero_accounts")

            logging.error(f"Xero accounts timer import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero accounts timer import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="30 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_contacts_import_timer(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO CONTACTS TIMER IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_contacts_for_connection,
            write_contacts_stage,
            merge_contacts,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for contacts import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading contacts for {tenant_name} ({connection_name})")
                rows = load_contacts_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_contacts_stage(conn, all_rows)
            merge_contacts(conn)
            update_sync_state(conn, "xero_contacts")

            logging.error(f"Xero contacts timer import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero contacts timer import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="40 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def xero_bank_transactions_import_timer(mytimer: func.TimerRequest) -> None:
    logging.error("=== XERO BANK TRANSACTIONS TIMER IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.loaders import (
            get_xero_connections,
            load_bank_transactions_for_connection,
            write_bank_transactions_stage,
            merge_bank_transactions,
        )

        conn = get_connection()

        try:
            all_conn_rows = get_xero_connections(conn)

            if not all_conn_rows:
                logging.error("No Xero connections found for bank transactions import")
                return

            all_rows = []
            for row in all_conn_rows:
                connection_name = row[0]
                tenant_id = row[1]
                tenant_name = row[2]

                logging.error(f"Loading bank transactions for {tenant_name} ({connection_name})")
                rows = load_bank_transactions_for_connection(connection_name, tenant_id, tenant_name)
                all_rows.extend(rows)

            write_bank_transactions_stage(conn, all_rows)
            merge_bank_transactions(conn)
            update_sync_state(conn, "xero_bank_transactions")

            logging.error(f"Xero bank transactions timer import complete. Rows loaded: {len(all_rows)}")

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Xero bank transactions timer import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise


@app.timer_trigger(schedule="50 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def fx_rates_gbp_timer(mytimer: func.TimerRequest) -> None:
    logging.error("=== GBP FX TIMER IMPORT STARTED ===")

    try:
        from xero.auth import get_connection
        from xero.fx_rates import (
            get_latest_gbp_rates,
            normalise_latest_payload_to_rows,
            upsert_fx_rates,
        )

        payload = get_latest_gbp_rates()
        rows = normalise_latest_payload_to_rows(payload)
        upsert_fx_rates(rows)

        conn = get_connection()
        try:
            update_sync_state(conn, "fx_rates_gbp_daily")
        finally:
            conn.close()

        logging.error(f"GBP FX timer import complete. Rows loaded: {len(rows)}")

    except Exception as e:
        logging.error(f"GBP FX timer import failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise

    logging.error("=== GBP FX TIMER IMPORT COMPLETED ===")
