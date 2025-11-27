import pandas as pd
import os
import calendar
import re
import pyodbc
from sqlalchemy import create_engine

# --- 🗂 Folder containing all monthly Excel files ---
folder_path = r'C:\Users\rushika\Downloads\Actual revenue ETL\Bank Collection'

# SQL Server connection using working pyodbc driver
server = r'localhost\RUSH'
database = 'DevServer'
username = 'sa'
password = 'Qwertyui123#'

# Create SQLAlchemy engine using the working pyodbc connection
conn_str = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)
engine = create_engine(conn_str)

# Table / schema
table_name = 'Bank_Payment_Collection'
schema_name = 'InsightStaging'

# List all Excel files in folder
excel_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".xlsx")]

if not excel_files:
    raise ValueError(f"❌ No Excel files found in {folder_path}")

# Columns to remove
unused_cols = ["Cash reveres", "VAT  REFUND", "PERCENTAGE"]

for file_name in excel_files:
    file_path = os.path.join(folder_path, file_name)
    print(f"\n📄 Processing file: {file_name}")

    # -------------------------
    # 🔍 Auto-detect YEAR
    # -------------------------
    year_match = re.search(r"\b(20\d{2})\b", file_name)
    if not year_match:
        print(f"⚠️ Could not detect year in filename '{file_name}', skipping.")
        continue
    year = int(year_match.group(1))

    # -------------------------
    # 🔍 Auto-detect MONTH
    # -------------------------
    month_names = list(calendar.month_name)[1:]
    month_name = next((m for m in month_names if m.lower() in file_name.lower()), None)
    if not month_name:
        print(f"⚠️ Could not detect month in filename '{file_name}', skipping.")
        continue
    month = month_names.index(month_name) + 1
    days_in_month = calendar.monthrange(year, month)[1]

    print(f"➡ Detected Year={year}, Month={month} ({month_name}), Days={days_in_month}")

    # -------------------------
    # 📥 Load Excel
    # -------------------------
    try:
        df_raw = pd.read_excel(file_path, usecols='A:M', header=3)
    except Exception as e:
        print(f"⚠️ Could not read Excel file '{file_name}': {e}")
        continue

    # -------------------------
    # 🔥 Drop first blank column if exists
    # -------------------------
    first_col = str(df_raw.columns[0]).strip()
    if first_col == "" or first_col.lower() == "unnamed: 0":
        df_raw = df_raw.drop(columns=[df_raw.columns[0]])

    # -------------------------
    # 🧹 Remove unused columns
    # -------------------------
    df = df_raw.drop(columns=[c for c in df_raw.columns if str(c).strip() in unused_cols], errors="ignore")

    # -------------------------
    # 🚫 Remove accidental Day / Blank columns
    # -------------------------
    drop_cols = [col for col in df.columns if str(col).strip().lower() in ["", "day"]]
    df = df.drop(columns=drop_cols, errors="ignore")

    # -------------------------
    # 🧮 Preserve decimal values exactly
    # -------------------------
    amount_cols = [c for c in df.columns if c not in ["Date", "Year", "Month"]]
    for col in amount_cols:
        df[col] = df[col].astype(str).str.replace(",", "", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # -------------------------
    # 🗓 Create Date column based on row count
    # -------------------------
    valid_row_count = min(len(df), days_in_month)
    df = df.head(valid_row_count)
    df["Date"] = pd.date_range(start=f"{year}-{month:02d}-01", periods=valid_row_count)

    # -------------------------
    # ➕ Add Year, Month
    # -------------------------
    df["Year"] = year
    df["Month"] = month

    # Move Date column to front
    cols = ["Date"] + [c for c in df.columns if c != "Date"]
    df = df[cols]

    # -------------------------
    # 💾 Save output locally (optional)
    # -------------------------
    output_name = file_name.replace(".xlsx", "_updated.xlsx")
    output_path = os.path.join(folder_path, output_name)
    df.to_excel(output_path, index=False, float_format="%.10f")
    print(f"✅ File processed and saved locally: {output_path}")

    # -------------------------
    # 💻 Append to SQL Server table
    # -------------------------
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            index=False,
            if_exists='append'
        )
        print(f"✅ Data appended to SQL Server: {schema_name}.{table_name}")
    except Exception as e:
        print(f"⚠️ Failed to write to SQL Server: {e}")

print("\n✅ All files processed and written to SQL Server successfully.")
