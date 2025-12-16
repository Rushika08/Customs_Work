import os
import re
import pandas as pd
from sqlalchemy import create_engine
import urllib
from dotenv import load_dotenv

# -------------------------------------------------------------
# 0. CONFIG
# -------------------------------------------------------------
FOLDER_PATH = r"C:\Users\rushika\Downloads\Actual revenue ETL"
EXCEL_EXTENSION = ".xlsx"

# -------------------------------------------------------------
# 1. SQL SERVER CONNECTION
# -------------------------------------------------------------
load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

if not all([server, database, username, password]):
    raise ValueError("❌ Database environment variables not loaded")

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
    "Encrypt=no;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
print("✅ Connection established with SQL Server.")

# -------------------------------------------------------------
# 2. PROCESS EACH EXCEL FILE
# -------------------------------------------------------------
excel_files = [
    os.path.join(FOLDER_PATH, f)
    for f in os.listdir(FOLDER_PATH)
    if f.lower().endswith(EXCEL_EXTENSION)
]

if not excel_files:
    raise ValueError("❌ No Excel files found in the folder")

print(f"📂 Found {len(excel_files)} Excel files")

for file_path in excel_files:
    print(f"\n📄 Processing file: {os.path.basename(file_path)}")

    # ---------------------------------------------------------
    # 3. READ EXCEL (4th row as header)
    # ---------------------------------------------------------
    df = pd.read_excel(file_path, header=3, usecols="A:N")
    df.columns = df.columns.str.strip()

    # ---------------------------------------------------------
    # 4. IDENTIFY VALID REVENUE CODE ROWS
    # ---------------------------------------------------------
    pattern = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
    valid_rows = df[df.iloc[:, 0].astype(str).str.match(pattern, na=False)]

    if valid_rows.empty:
        print("⚠️ No valid revenue codes found. Skipping file.")
        continue

    last_index = valid_rows.index[-1]
    df = df.loc[:last_index].copy()

    # ---------------------------------------------------------
    # 5. CLEAN VALUES
    # ---------------------------------------------------------
    df = df.replace("-", pd.NA)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df.iloc[:, 1] = df.iloc[:, 1].fillna(df.iloc[:, 0])

    # ---------------------------------------------------------
    # 6. SPLIT FIXED & MONTHLY COLUMNS
    # ---------------------------------------------------------
    fixed_cols = df.columns[:2]
    month_cols = df.columns[2:]

    # ---------------------------------------------------------
    # 7. MELT DATA
    # ---------------------------------------------------------
    df_melted = df.melt(
        id_vars=fixed_cols,
        value_vars=month_cols,
        var_name="Month",
        value_name="Value"
    )

    # ---------------------------------------------------------
    # 8. EXTRACT YEAR FROM FILE NAME
    # ---------------------------------------------------------
    file_name = os.path.basename(file_path)
    match = re.search(r"(\d{4})", file_name)

    if not match:
        print("⚠️ Year not found in file name. Skipping file.")
        continue

    year = int(match.group(1))
    df_melted["Year"] = year
    df_melted["Date"] = df_melted["Year"].astype(str) + " " + df_melted["Month"]

    # ---------------------------------------------------------
    # 9. FINAL FORMAT
    # ---------------------------------------------------------
    df_final = df_melted.rename(columns={
        df.columns[0]: "Revenue_Code",
        df.columns[1]: "Revenue_Source"
    })[["Year", "Month", "Revenue_Code", "Revenue_Source", "Value"]]

    df_final["Value"] = pd.to_numeric(df_final["Value"], errors="coerce")

    # ---------------------------------------------------------
    # 10. ENSURE ALL 12 MONTHS EXIST
    # ---------------------------------------------------------
    month_order = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    unique_codes = df_final["Revenue_Code"].unique()

    full_index = pd.MultiIndex.from_product(
        [unique_codes, month_order],
        names=["Revenue_Code", "Month"]
    )

    df_final = (
        df_final
        .set_index(["Revenue_Code", "Month"])
        .reindex(full_index)
        .reset_index()
    )

    df_final["Year"] = year

    # ---------------------------------------------------------
    # 11. LOAD INTO SQL
    # ---------------------------------------------------------
    df_final.to_sql(
        name="Actual_Revenue",
        con=engine,
        schema="InsightStaging",
        if_exists="append",
        index=False
    )

    print(f"✅ Loaded {len(df_final)} rows for year {year}")

print("\n🎉 All files processed successfully.")
