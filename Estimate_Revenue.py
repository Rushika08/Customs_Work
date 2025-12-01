import os
import re
import pandas as pd
from sqlalchemy import create_engine

# -------------------------------------------------------------
# 1. SQL SERVER CONNECTION
# -------------------------------------------------------------
server = r'localhost\RUSH'
database = 'DevServer'
username = 'sa'
password = 'Qwertyui123#'

connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(connection_string)
print("✅ Connection established with SQL Server.")

# -------------------------------------------------------------
# 2. READ EXCEL WITH 4TH ROW AS HEADER (A, B, D–O)
# -------------------------------------------------------------
file_path = r'C:\Users\rushika\Downloads\Actual revenue ETL\Estimate Revenues\2022 Estimate Revenue.xlsx'

df = pd.read_excel(file_path, header=3, usecols="A,B,D:O")
df.columns = df.columns.str.strip()

# -------------------------------------------------------------
# 3. FILTER ONLY VALID REVENUE CODE ROWS (xxxx.xx.xx)
# -------------------------------------------------------------
pattern = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
revenue_code_col = df.columns[0]

df = df[df[revenue_code_col].astype(str).str.match(pattern, na=False)]
df.reset_index(drop=True, inplace=True)

# -------------------------------------------------------------
# 4. CLEAN VALUES
# -------------------------------------------------------------
df = df.replace("-", pd.NA)
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# Fill missing Revenue Source using Revenue Code
df.iloc[:, 1] = df.iloc[:, 1].fillna(df.iloc[:, 0])

# -------------------------------------------------------------
# 5. SPLIT FIXED & MONTHLY COLUMNS
# -------------------------------------------------------------
fixed_cols = df.columns[:2]   # Revenue Code, Revenue Source
month_cols = df.columns[2:]   # Jan–Dec columns

# -------------------------------------------------------------
# 6. MELT TO LONG FORMAT
# -------------------------------------------------------------
df_melted = df.melt(
    id_vars=fixed_cols,
    value_vars=month_cols,
    var_name="Month",
    value_name="Value"
)

# -------------------------------------------------------------
# 7. EXTRACT YEAR AUTOMATICALLY FROM FILE NAME
# -------------------------------------------------------------
file_name = os.path.basename(file_path)
match = re.search(r"(\d{4})", file_name)

if not match:
    raise ValueError("❌ No 4-digit year found in the file name.")

extracted_year = int(match.group(1))
df_melted["Year"] = extracted_year

# -------------------------------------------------------------
# 8. FINAL RENAME + STRUCTURE
# -------------------------------------------------------------
df_final = df_melted.rename(columns={
    df.columns[0]: "Revenue Code",
    df.columns[1]: "Revenue Source"
})[["Year", "Month", "Revenue Code", "Revenue Source", "Value"]]

# -------------------------------------------------------------
# 9. ENSURE ALL 12 MONTHS EXIST
# -------------------------------------------------------------
month_order = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

unique_codes = df_final["Revenue Code"].unique()
reindex_template = pd.MultiIndex.from_product(
    [unique_codes, month_order],
    names=["Revenue Code", "Month"]
)

df_final = (
    df_final
    .set_index(["Revenue Code", "Month"])
    .reindex(reindex_template)
    .reset_index()
)

df_final["Year"] = extracted_year  # restore year

# -------------------------------------------------------------
# 10. REORDER & CLEAN
# -------------------------------------------------------------
df_final = df_final[["Year", "Month", "Revenue Code", "Revenue Source", "Value"]]

df_final = df_final.rename(columns={
    "Revenue Code": "Revenue_Code",
    "Revenue Source": "Revenue_Source"
})

df_final["Value"] = pd.to_numeric(df_final["Value"], errors="coerce")

# -------------------------------------------------------------
# 11. PREVIEW
# -------------------------------------------------------------
print(df_final.head(20))
print(f"\nTotal rows after ensuring 12 months per Revenue Code: {len(df_final)}")

# -------------------------------------------------------------
# 12. LOAD INTO SQL
# -------------------------------------------------------------
table_name = "Estimate_Revenue"
schema_name = "InsightStaging"

df_final.to_sql(
    table_name,
    con=engine,
    schema=schema_name,
    if_exists='append',
    index=False
)

print(f"✅ Successfully loaded {len(df_final)} rows into table '{schema_name}.{table_name}'.")
