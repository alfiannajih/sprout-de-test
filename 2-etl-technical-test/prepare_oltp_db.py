import pandas as pd
import os
from sqlite3 import connect

def parse_sheet(xl, sheet_name):
    df = xl.parse(sheet_name)
    parsed_df = df.iloc[:, 0].str.split(",", expand=True)
    parsed_df.columns = df.columns[0].split(",")

    return parsed_df

def main():
    
    path = "./data_source/etl_test_source.xlsx"
    xl = pd.ExcelFile(path)

    if not os.path.exists("db"):
        os.makedirs("db")

    for sheet_name in xl.sheet_names:
        with connect("db/oltp.db") as conn:
            df = parse_sheet(xl, sheet_name)
            df.to_sql(sheet_name, conn, if_exists="replace", index=False)

    xl.close()

if __name__ == "__main__":
    main()