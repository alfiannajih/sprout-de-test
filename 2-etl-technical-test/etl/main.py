import os
import pandas as pd
from typing import Dict
from datetime import datetime
import argparse

from utils import (
    extract_from_oltp_db,
    init_scd_table_in_olap,
    read_scd_table_in_olap,
    compare_tables,
    inserting_scd_table_in_olap,
    expiring_scd_table_in_olap,
    user_profiling_columns,
    transaction_summary_columns
)


def extract() -> dict:
    """
    Extracts data from OLTP database into a dictionary of pandas DataFrames
    
    Returns:
        dict: A dictionary of pandas DataFrames with the following keys:
            users: DataFrame containing user data
            transactions: DataFrame containing transaction data
            membership_purchases: DataFrame containing membership purchase data
            user_activity: DataFrame containing user activity data
            mdr_data: DataFrame containing MDR data
    """
    user_df = extract_from_oltp_db("users", ["User_ID", "Name", "Email"])
    transaction_df = extract_from_oltp_db("transactions", ["Transaction_ID", "User_ID", "Transaction_Amount", "Timestamp"])
    membership_df = extract_from_oltp_db("membership_purchases", ["Membership_ID", "User_ID", "Membership_Type", "Purchase_Date", "Expiry_Date"])
    user_activity_df = extract_from_oltp_db("user_activity", ["Activity_ID", "User_ID", "Activity_Type", "Activity_Date"])
    mdr_df = extract_from_oltp_db("mdr_data", ["Membership_Type", "MDR_Percentage"])

    dfs = {
        "users": user_df,
        "transactions": transaction_df,
        "membership_purchases": membership_df,
        "user_activity": user_activity_df,
        "mdr_data": mdr_df
    }

    return dfs


def transform_user_profiling(
    dfs: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Transforms user profiling data by aggregating and merging information
    from multiple data sources.

    Args:
        dfs (Dict[str, pd.DataFrame]): A dictionary of pandas DataFrames with keys:
            - "users": User information
            - "transactions": Transaction records
            - "membership_purchases": Membership purchase details
            - "user_activity": User activity logs

    Returns:
        pd.DataFrame: A DataFrame containing aggregated user profiling data with columns:
            - user_id
            - name
            - email
            - phone
            - first_transaction_date
            - last_transaction_date
            - total_transactions
            - total_spent
            - last_membership
            - last_membership_expiry_date
            - basic_membership_duration_days
            - premium_membership_duration_days
            - last_activity
            - last_activity_date
    """
    user_df = dfs.get("users")
    transaction_df = dfs.get("transactions")
    membership_df = dfs.get("membership_purchases")
    user_activity_df = dfs.get("user_activity")

    # Transoform user-transaction related data 
    if not transaction_df.empty:
        user_first_tx = transaction_df.groupby("User_ID")["Timestamp"].min().reset_index()
        user_first_tx.columns = ["User_ID", "First_Transaction_Date"]

        user_last_tx = transaction_df.groupby("User_ID")["Timestamp"].max().reset_index()
        user_last_tx.columns = ["User_ID", "Last_Transaction_Date"]

        transaction_df["Transaction_Amount"] = transaction_df["Transaction_Amount"].astype(float)
        user_transaction_stats = transaction_df.groupby("User_ID").agg(
            Total_Transactions=("Transaction_ID", "count"),
            Total_Spent=("Transaction_Amount", "sum")
        ).reset_index()

    # Transoform user-membership related data
    if not membership_df.empty:
        last_membership = membership_df.loc[membership_df.groupby("User_ID")["Expiry_Date"].idxmax(), ["User_ID", "Membership_Type", "Expiry_Date"]]
        last_membership.columns = ["User_ID", "Last_Membership", "Last_Membership_Expiry_Date"]

        membership_df["Purchase_Date"] = pd.to_datetime(membership_df["Purchase_Date"])
        membership_df["Expiry_Date"] = pd.to_datetime(membership_df["Expiry_Date"])

        membership_df["Duration_Days"] = (membership_df["Expiry_Date"] - membership_df["Purchase_Date"]).dt.days

        membership_duration = membership_df.pivot_table(
            index="User_ID",
            columns="Membership_Type",
            values="Duration_Days",
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        membership_duration.columns = ["User_ID", "Basic_Membership_Duration_Days", "Premium_Membership_Duration_Days"]

    # Transoform user-activity related data
    if not user_activity_df.empty:
        last_activity = user_activity_df.loc[user_activity_df.groupby("User_ID")["Activity_Date"].idxmax(), ["User_ID", "Activity_Type", "Activity_Date"]]
        last_activity.columns = ["User_ID", "Last_Activity", "Last_Activity_Date"]

    # Merge transformed data
    user_profiling = user_df \
        .merge(user_first_tx, on="User_ID", how="left") \
        .merge(user_last_tx, on="User_ID", how="left") \
        .merge(user_transaction_stats, on="User_ID", how="left") \
        .merge(last_membership, on="User_ID", how="left") \
        .merge(membership_duration, on="User_ID", how="left") \
        .merge(last_activity, on="User_ID", how="left")
    
    # Ensure correct data types for each columns
    string_columns = ["Name", "Email", "Phone", "Last_Membership", "Last_Activity"]
    integer_columns = ["User_ID", "Total_Transactions", "Basic_Membership_Duration_Days", "Premium_Membership_Duration_Days"]
    float_columns = ["Total_Spent"]
    date_columns = ["First_Transaction_Date", "Last_Transaction_Date", "Last_Activity_Date", "Last_Membership_Expiry_Date"]

    for col in string_columns:
        user_profiling[col] = user_profiling[col].astype(str).replace({"nan": None})

    for col in integer_columns:
        user_profiling[col] = pd.to_numeric(user_profiling[col], errors="coerce").fillna(0).astype(int)

    for col in float_columns:
        user_profiling[col] = pd.to_numeric(user_profiling[col]).fillna(0).astype(float)

    for col in date_columns:
        user_profiling[col] = pd.to_datetime(user_profiling[col]).replace({pd.NaT: None})
    
    # Rename columns to lowercase
    user_profiling.columns = [col.lower() for col in user_profiling.columns]

    return user_profiling


def transform_transaction_summary(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transforms transaction summary data by aggregating and merging information
    from multiple data sources.

    Args:
        dfs (Dict[str, pd.DataFrame]): A dictionary of pandas DataFrames with keys:
            - "transactions": Transaction records
            - "membership_purchases": Membership purchase details
            - "mdr_data": MDR data

    Returns:
        pd.DataFrame: A DataFrame containing aggregated transaction summary data with columns:
            - membership_type
            - total_transactions
            - total_amount
            - mdr_revenue
    """
    transaction_df = dfs.get("transactions")
    membership_df = dfs.get("membership_purchases")
    mdr_df = dfs.get("mdr_data")

    # Summarize transaction data
    transaction_df["Transaction_Amount"] = transaction_df["Transaction_Amount"].astype(float)
    transaction_summary = transaction_df.merge(membership_df, on="User_ID") \
        .merge(mdr_df, on="Membership_Type") \
        .groupby("Membership_Type").agg(
            Total_Transactions=("Transaction_ID", "count"),
            Total_Amount=("Transaction_Amount", "sum"),
            MDR_Revenue=("Transaction_Amount", lambda x: (x.astype(float) * float(mdr_df["MDR_Percentage"].iloc[0]) / 100).sum())
        ).reset_index()
    
    # Ensure correct data types for each columns
    string_columns = ["Membership_Type"]
    integer_columns = ["Total_Transactions"]
    float_columns = ["Total_Amount", "MDR_Revenue"]

    for col in string_columns:
        transaction_summary[col] = transaction_summary[col].astype(str).replace({"nan": None})

    for col in integer_columns:
        transaction_summary[col] = pd.to_numeric(transaction_summary[col], errors="coerce").fillna(0).astype(int)

    for col in float_columns:
        transaction_summary[col] = pd.to_numeric(transaction_summary[col]).fillna(0).astype(float)

    # Rename columns to lowercase
    transaction_summary.columns = [col.lower() for col in transaction_summary.columns]

    return transaction_summary


def load_user_profiling(df: pd.DataFrame):
    """
    Loads user profiling data into SCD type 2 table in OLAP database.

    Args:
        df (pd.DataFrame): A DataFrame containing user profiling data with columns:

    Returns:
        None
    """
    # Add effective_start_date, effective_end_date, and is_active columns for history tracking
    df["effective_start_date"] = datetime.now()
    df["effective_end_date"] = pd.NaT
    df["is_active"] = True

    # Create table in OLAP database if it doesn't exist
    init_scd_table_in_olap(user_profiling_columns, "user_profiling", "user_sk")
    
    # Compare existing data with new data
    existing_df = read_scd_table_in_olap("user_profiling")
    if not existing_df.empty:
        rows_to_insert, rows_to_update, deleted_rows = compare_tables(df, existing_df, "user_id")
    else:
        rows_to_insert, rows_to_update, deleted_rows = df, pd.DataFrame(), pd.DataFrame()

    # Insert and update corresponding data
    if not rows_to_update.empty:
        expiring_scd_table_in_olap("user_sk", rows_to_update["user_sk"], "user_profiling")
        inserting_scd_table_in_olap("user_profiling", user_profiling_columns, rows_to_update.to_dict('records'))

    if not deleted_rows.empty:
        expiring_scd_table_in_olap("user_sk", deleted_rows["user_sk"], "user_profiling")

    if not rows_to_insert.empty:
        inserting_scd_table_in_olap("user_profiling", user_profiling_columns, rows_to_insert.to_dict('records'))


def load_transaction_summary(df: pd.DataFrame):
    """
    Loads the transaction summary data into the transaction_summary SCD table in the OLAP database.

    Args:
        df: The DataFrame containing the transaction summary data.

    Returns:
        None
    """
    # Add effective_start_date, effective_end_date, and is_active columns for history tracking
    df["effective_start_date"] = datetime.now()
    df["effective_end_date"] = pd.NaT
    df["is_active"] = True
    
    # Create table in OLAP database if it doesn't exist
    init_scd_table_in_olap(transaction_summary_columns, "transaction_summary", "transaction_summary_sk")

    # Compare existing data with new data
    existing_df = read_scd_table_in_olap("transaction_summary")
    if not existing_df.empty:
        rows_to_insert, rows_to_update, deleted_rows = compare_tables(df, existing_df, "membership_type")
    else:
        rows_to_insert, rows_to_update, deleted_rows = df, pd.DataFrame(), pd.DataFrame()

    # Insert and update corresponding data
    if not rows_to_update.empty:
        expiring_scd_table_in_olap("transaction_summary_sk", rows_to_update["transaction_summary_sk"], "transaction_summary")
        inserting_scd_table_in_olap("transaction_summary", transaction_summary_columns, rows_to_update.to_dict('records'))

    if not deleted_rows.empty:
        expiring_scd_table_in_olap("transaction_summary_sk", deleted_rows["transaction_summary_sk"], "transaction_summary")

    if not rows_to_insert.empty:
        inserting_scd_table_in_olap("transaction_summary", transaction_summary_columns, rows_to_insert.to_dict('records'))


def generate_xlsx_report(df: pd.DataFrame, sheet_name: str, file_path: str):
    """
    Saves the given DataFrame as an Excel sheet in the given file path.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        sheet_name (str): The name of the Excel sheet.
        file_path (str): The path to save the Excel file.

    Returns:
        None
    """
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    if not os.path.exists(file_path):
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    else:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)


def main(args):
    """
    The main entry point of the ETL pipeline.

    This function performs the following steps in order:
    1. Extracts data from the source Excel file.
    2. Transforms the data into user profiling and transaction summary data frames.
    3. Loads the transformed data into the OLAP database.
    4. Saves the transformed data into an Excel report.

    Args:
        args (argparse.Namespace): The command line arguments.
    """
    print(f"ETL is starting.")

    dfs = extract()
    
    user_profiling = transform_user_profiling(dfs)
    transaction_summary = transform_transaction_summary(dfs)

    load_user_profiling(user_profiling)
    load_transaction_summary(transaction_summary)

    output_path = f"reports/{args.date}.xlsx"

    user_profiling_selected_columns = [
        "user_id",
        "name",
        "email",
        "phone",
        "first_transaction_date",
        "last_transaction_date",
        "total_transactions",
        "total_spent",
        "last_membership",
        "last_membership_expiry_date",
        "basic_membership_duration_days",
        "premium_membership_duration_days",
        "last_activity",
        "last_activity_date",
    ]
    df = read_scd_table_in_olap("user_profiling", columns=user_profiling_selected_columns)
    generate_xlsx_report(df, "user_profiling", output_path)

    transaction_summary_selected_columns = [
        "membership_type",
        "total_transactions",
        "total_amount",
        "mdr_revenue",
    ]
    df = read_scd_table_in_olap("transaction_summary", columns=transaction_summary_selected_columns)
    generate_xlsx_report(df, "transaction_summary", output_path)

    print(f"ETL is finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True)
    args = parser.parse_args()
    main(args)