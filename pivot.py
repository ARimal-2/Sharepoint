

import pandas as pd
import numpy as np


FILE_NAME = "FY26 Master Planning File.xlsx"
SHEET_NAME = "Forecasts"
USECOLS = "DP:EP"
HEADER_ROW = 2
NROWS = 56


def swap_columns(df, col1, col2):
    cols = list(df.columns)
    i, j = cols.index(col1), cols.index(col2)
    cols[i], cols[j] = cols[j], cols[i]
    return df[cols]


df = pd.read_excel(
    FILE_NAME,
    sheet_name=SHEET_NAME,
    usecols=USECOLS,
    index_col=False,
    header=HEADER_ROW,
    nrows=NROWS,
    dtype=str
)


df_t = df.transpose()

# First row becomes header
df_t.columns = df_t.iloc[0]
df_t = df_t.iloc[1:]

# Drop columns with NaN header
df_t = df_t.loc[:, df_t.columns.notna()]

# Reset index → Period
df_t = df_t.reset_index().rename(columns={"index": "Period"})

# Drop unwanted columns
df_t = df_t.drop(columns=["Cases / Pallet", "Case Weight"], errors="ignore")

# Swap Period and SKU
df_t = swap_columns(df_t, "Period", "SKU")

#
# FIX 1: Normalize SKU (remove .0 from Excel headers)

df_t["SKU"] = (
    df_t["SKU"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
)


VALUE_COLS = [c for c in df_t.columns if c not in ["SKU", "Period"]]

for col in VALUE_COLS:
    df_t[col] = (
        pd.to_numeric(df_t[col], errors="coerce")
          .pipe(np.round)        
          .astype("Int64")      
          .fillna(0)
    )



print("before transpose")
print(df.head())
print("\nFinal data preview:")
print(df_t.head())

