import pandas as pd
import os
import calendar
import re
import pyodbc
from sqlalchemy import create_engine
import urllib
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# --- 🗂 Folder containing all monthly Excel files ---
folder_path = r'E:\Revenue Sources\Bank Sheets'

# --- 📂 Output folder for loaded Excel files ---
output_folder = r'E:\Revenue Sources Loaded\Bank Sheets'
os.makedirs(output_folder, exist_ok=True)


# 🔐 Read credentials from .env
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# 🚨 Safety check
if not all([server, database, username, password]):
    raise ValueError("❌ Database environment variables not loaded")

# ✅ WORKING SQLALCHEMY CONNECTION (odbc_connect)
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
    df = df_raw.drop(
        columns=[c for c in df_raw.columns if str(c).strip() in unused_cols],
        errors="ignore"
    )

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
    # 🗓 Create Date column
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
    output_name = file_name.replace(".xlsx", "_loaded.xlsx")
    output_path = os.path.join(output_folder, output_name)

    df.to_excel(output_path, index=False, float_format="%.10f")
    print(f"✅ File processed and saved to loaded folder: {output_path}")


    # -------------------------
    # 💻 Append to SQL Server
    # -------------------------
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            index=False,
            if_exists='append',
            method='multi'  # 🚀 faster inserts
        )
        sql_success = True
        print(f"✅ Data appended to SQL Server: {schema_name}.{table_name}")
    except Exception as e:
        print(f"⚠️ Failed to write to SQL Server: {e}")

    # -------------------------
    # 🗑 Delete source file ONLY if SQL succeeded
    # -------------------------
    if sql_success:
        try:
            os.remove(file_path)
            print(f"🗑 Source file deleted: {file_path}")
        except Exception as e:
            print(f"⚠️ Could not delete source file: {e}")
    else:
        print(f"🚫 Source file NOT deleted due to processing failure: {file_path}")

print("\n🎉 All files processed and written to SQL Server successfully.")
