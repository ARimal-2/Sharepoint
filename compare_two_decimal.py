
# import pandas as pd
# import numpy as np

# FILE_NAME = "FY26 Master Planning File.xlsx"
# SHEET_NAME = "Forecasts"
# USECOLS = "BK:DN"
# HEADER_ROW = 2
# NROWS = 56

# # Read original sheet
# df = pd.read_excel(
#     FILE_NAME,
#     sheet_name=SHEET_NAME,
#     usecols=USECOLS,
#     index_col=False,
#     header=HEADER_ROW,
#     nrows=NROWS,
#     dtype=str
# )

# # Transpose (your previous code)
# df_t = df.transpose()
# df_t.columns = df_t.iloc[0]
# df_t = df_t.iloc[1:]
# df_t = df_t.loc[:, df_t.columns.notna()]
# df_t = df_t.reset_index().rename(columns={"index": "Period"})
# df_t = df_t.drop(columns=["Cases / Pallet", "Case Weight"], errors="ignore")
# df_t["SKU"] = df_t["SKU"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

# # Identify value columns
# VALUE_COLS = [c for c in df_t.columns if c not in ["SKU", "Period"]]

# # Convert numeric values
# for col in VALUE_COLS:
#     df_t[col] = pd.to_numeric(df_t[col], errors="coerce").pipe(np.round).astype("Int64").fillna(0)

# # --- Verification ---
# mismatches = []

# # Map original rows to transposed column names
# # Skip header row and metadata rows (0 = SKU, 2 = Cases/Pallet, 3 = Case Weight)
# original_data_rows = df.iloc[4:, 1:]  # data only, skip first column (metadata)

# for col_idx, sku_val in enumerate(df.iloc[0, 1:]):  # SKU headers
#     sku = str(sku_val).replace(".0", "")
#     for row_idx in range(original_data_rows.shape[0]):
#         original_val = original_data_rows.iloc[row_idx, col_idx]
#         period = original_data_rows.index[row_idx]  # this will be row number in original sheet
        
#         # Get corresponding column in transposed DataFrame
#         try:
#             transposed_col = df_t.columns[row_idx + 2]  # +2 because Period & SKU columns
#         except IndexError:
#             # skip if transposed column not found (due to dropped columns)
#             continue

#         transposed_val = df_t.loc[df_t['SKU'] == sku, transposed_col].values[0]

#         # Normalize NaN
#         if pd.isna(original_val): original_val = np.nan
#         if pd.isna(transposed_val): transposed_val = np.nan

#         if str(original_val) != str(transposed_val):
#             mismatches.append({
#                 "SKU": sku,
#                 "Period": transposed_col,
#                 "Original": original_val,
#                 "Transposed": transposed_val
#             })

# print(f"Total mismatches found: {len(mismatches)}")
# if mismatches:
#     print("Sample mismatches (up to 10):")
#     for m in mismatches[:]:
#         print(m)
# else:
#     print("Transpose verification passed! All values match.")
