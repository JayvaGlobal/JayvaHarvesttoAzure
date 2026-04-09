from sqlalchemy import create_engine
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
