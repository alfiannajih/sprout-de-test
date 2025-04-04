import sqlite3
from datetime import datetime
import duckdb
import numpy as np
import pandas as pd
import os
from typing import List, Tuple

user_profiling_columns = {
    "user_id": "INTEGER",
    "name": "VARCHAR",
    "email": "VARCHAR",
    "phone": "VARCHAR",
    "first_transaction_date": "TIMESTAMP",
    "last_transaction_date": "TIMESTAMP",
    "total_transactions": "INTEGER",
    "total_spent": "NUMERIC",
    "last_membership": "VARCHAR",
    "last_membership_expiry_date": "TIMESTAMP",
    "basic_membership_duration_days": "INTEGER",
    "premium_membership_duration_days": "INTEGER",
    "last_activity": "VARCHAR",
    "last_activity_date": "TIMESTAMP",
    "effective_start_date": "TIMESTAMP",
    "effective_end_date": "TIMESTAMP",
    "is_active": "BOOLEAN",
}

transaction_summary_columns = {
    "membership_type": "VARCHAR",
    "total_transactions": "INTEGER",
    "total_amount": "NUMERIC",
    "mdr_revenue": "NUMERIC",
    "effective_start_date": "TIMESTAMP",
    "effective_end_date": "TIMESTAMP",
    "is_active": "BOOLEAN",
}

OLTP_PATH = "db/oltp.db"
OLAP_PATH = "db/olap.db"

def extract_from_oltp_db(
    table_name: str,
    non_nullable_columns: list = None
):
    with sqlite3.connect(OLTP_PATH) as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
        df = df.replace({
            "": np.nan
        })

        if non_nullable_columns:
            for column in non_nullable_columns:
                df.dropna(subset=[column], inplace=True)

        return df


def init_scd_table_in_olap(
    columns: dict,
    table_name: str,
    surrogate_key: str
):
    with duckdb.connect(OLAP_PATH) as conn:
        try:
            conn.execute("BEGIN TRANSACTION")

            conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {surrogate_key}_seq START 1")

            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {surrogate_key} INTEGER PRIMARY KEY DEFAULT NEXTVAL('{surrogate_key}_seq'),
                    {', '.join([f'{k} {v}' for k, v in columns.items()])}
                );
            """)

            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            raise e
    

def read_scd_table_in_olap(
    table_name: str,
    columns: list = ["*"]
) -> list:
    if len(columns) == 1:
        selected_columns = columns[0]
    else:
        selected_columns = ", ".join(columns)

    with duckdb.connect(OLAP_PATH) as conn:
        df = conn.execute(f"SELECT {selected_columns} FROM {table_name} WHERE is_active = 1").fetch_df()
        df = df.replace({
            pd.NaT: None
        })

    return df


def expiring_scd_table_in_olap(
    surrogate_key: str,
    surrogate_key_values: list,
    table_name: str,
):
    with duckdb.connect(OLAP_PATH) as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            # Expiring rows (Update effective_end_date and is_active)
            where_statement = str(tuple(surrogate_key_values)) if len(surrogate_key_values) > 1 else f"({str(surrogate_key_values.iloc[0])})"

            conn.execute(f"""
                UPDATE {table_name}
                SET effective_end_date = '{datetime.now()}', is_active = 0
                WHERE {surrogate_key} IN {where_statement};
            """)

            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            raise e


def inserting_scd_table_in_olap(
    table_name: str,
    columns: list,
    data: List[dict]
):
    with duckdb.connect(OLAP_PATH) as conn:
        try:
            conn.execute("BEGIN TRANSACTION")

            # Inserting new rows
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(['?'] * len(columns))})
            """
            bulk_data = []
            for row in data:
                bulk_data.append(tuple(row.values()))

            conn.executemany(insert_query, bulk_data)

            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            raise e
    

def compare_tables(
    new_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    natural_key: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Decide which rows to be inserted
    merged_df = pd.merge(
        new_df,
        existing_df,
        on=[natural_key],
        suffixes=("", "_old"),
        how="outer",
        indicator=True
    )
    rows_to_insert = merged_df[merged_df['_merge'] == 'left_only'][new_df.columns]
    deleted_rows = merged_df[merged_df['_merge'] == 'right_only']

    # Decide which rows to be updated
    columns_to_compare = new_df.columns[1:-3]

    # Dynamically create the condition for rows to update
    update_condition = False
    for col in columns_to_compare:
        update_condition |= (~merged_df[f'{col}'].eq(merged_df[f'{col}_old']))

    # Filter rows to update
    rows_to_update = merged_df[(merged_df['_merge'] == 'both') & update_condition]

    return (rows_to_insert, rows_to_update, deleted_rows)