from datetime import datetime, timezone, timedelta
import requests

from .auth import get_connection


FRANKFURTER_URL = "https://api.frankfurter.dev/v2/rates"


def get_latest_gbp_rates():
    response = requests.get(
        FRANKFURTER_URL,
        params={
            "base": "GBP",
            "quotes": "AUD,NZD,USD,EUR,GBP",
            "providers": "ECB",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def get_historical_gbp_rates(start_date: str, end_date: str):
    response = requests.get(
        FRANKFURTER_URL,
        params={
            "base": "GBP",
            "quotes": "AUD,NZD,USD,EUR,GBP",
            "from": start_date,
            "to": end_date,
            "providers": "ECB",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def upsert_fx_rates(rows):
    conn = get_connection()
    try:
        cursor = conn.cursor()

        merge_sql = """
        MERGE rpt.fx_rates_gbp_daily AS target
        USING (
            SELECT
                ? AS rate_date,
                ? AS source_currency,
                ? AS reporting_currency,
                ? AS fx_rate,
                ? AS source_name
        ) AS src
        ON target.rate_date = src.rate_date
           AND target.source_currency = src.source_currency
           AND target.reporting_currency = src.reporting_currency
           AND target.source_name = src.source_name
        WHEN MATCHED THEN
            UPDATE SET
                fx_rate = src.fx_rate,
                loaded_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                rate_date,
                source_currency,
                reporting_currency,
                fx_rate,
                source_name,
                loaded_at_utc
            )
            VALUES (
                src.rate_date,
                src.source_currency,
                src.reporting_currency,
                src.fx_rate,
                src.source_name,
                SYSUTCDATETIME()
            );
        """

        for row in rows:
            cursor.execute(
                merge_sql,
                (
                    row["rate_date"],
                    row["source_currency"],
                    row["reporting_currency"],
                    row["fx_rate"],
                    row["source_name"],
                ),
            )

        conn.commit()
    finally:
        conn.close()


def normalise_latest_payload_to_rows(payload):
    rows = []

    rate_date = payload.get("date")
    rates = payload.get("rates", {})

    # Frankfurter with base=GBP returns rates as target-per-GBP.
    # For reporting we want source -> GBP.
    # So if payload says 1 GBP = 1.95 AUD, then AUD -> GBP = 1 / 1.95.
    for target_currency, rate in rates.items():
        if target_currency == "GBP":
            rows.append({
                "rate_date": rate_date,
                "source_currency": "GBP",
                "reporting_currency": "GBP",
                "fx_rate": 1.0,
                "source_name": "ECB",
            })
        else:
            rows.append({
                "rate_date": rate_date,
                "source_currency": target_currency,
                "reporting_currency": "GBP",
                "fx_rate": float(1 / rate),
                "source_name": "ECB",
            })

    # Ensure GBP->GBP exists even if not returned
    if not any(r["source_currency"] == "GBP" for r in rows):
        rows.append({
            "rate_date": rate_date,
            "source_currency": "GBP",
            "reporting_currency": "GBP",
            "fx_rate": 1.0,
            "source_name": "ECB",
        })

    return rows


def normalise_historical_payload_to_rows(payload):
    rows = []
    all_rates = payload.get("rates", {})

    for rate_date, daily in all_rates.items():
        for target_currency, rate in daily.items():
            if target_currency == "GBP":
                rows.append({
                    "rate_date": rate_date,
                    "source_currency": "GBP",
                    "reporting_currency": "GBP",
                    "fx_rate": 1.0,
                    "source_name": "ECB",
                })
            else:
                rows.append({
                    "rate_date": rate_date,
                    "source_currency": target_currency,
                    "reporting_currency": "GBP",
                    "fx_rate": float(1 / rate),
                    "source_name": "ECB",
                })

        if not any(r["rate_date"] == rate_date and r["source_currency"] == "GBP" for r in rows):
            rows.append({
                "rate_date": rate_date,
                "source_currency": "GBP",
                "reporting_currency": "GBP",
                "fx_rate": 1.0,
                "source_name": "ECB",
            })

    return rows
