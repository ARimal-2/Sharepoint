# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.functions import trim, when
from pyspark.sql import SparkSession, functions as F
from sharepoint_transformation.vert_san_plan import vert_san_plan_transform_and_load
from sharepoint_transformation.vert_alex_plan import vert_alex_plan_transform_and_load
from sharepoint_transformation.vert_ster_plan import vert_ster_plan_transform_and_load
from sharepoint_transformation.ntiva_lookup import ntiva_lookup_load
from pyspark.sql import functions as F, Window

# # ---------------------------------------------------------
# # Column name standardization
# # ---------------------------------------------------------
# def clean_column_names(df):
#     def clean(col_name: str) -> str:
#         return (
#             col_name.strip()
#                     .replace(" ", "_")
#                     .replace(".", "_")
#                     .replace("-", "_")
#                     .replace("/", "_")
#         )

#     return df.select([F.col(c).alias(clean(c)) for c in df.columns])


# # ---------------------------------------------------------
# # Normalize Excel placeholders into NULL
# # ---------------------------------------------------------
# def normalize_nulls(df):
#     """
#     Convert Excel placeholders like '-', '', ' ' into true Spark NULLs.
#     """
#     for c in df.columns:
#         df = df.withColumn(
#             c,
#             when(trim(F.col(c)).isin("-", ""), None)
#             .otherwise(F.col(c))
#         )
#     return df


# # ---------------------------------------------------------
# # Automatically cast numeric planning columns
# # ---------------------------------------------------------
# def cast_numeric_columns(df):
#     """
#     Converts numeric-looking planning columns into DOUBLE.
#     This prevents Iceberg from storing numbers as strings.
#     """
#     for c in df.columns:
#         # Typical planning columns like FY26_Q1, FY26_Total, Jan_2026, etc
#         if any(x in c.lower() for x in ["fy", "total", "jan", "feb", "mar", "apr", "may", "jun",
#                                         "jul", "aug", "sep", "oct", "nov", "dec"]):
#             df = df.withColumn(c, F.col(c).cast("double"))

#     return df


# # ---------------------------------------------------------
# # Core processing logic
# # ---------------------------------------------------------
# def process_table(spark, transform_func):

#     # Get metadata from each table config
#     _, full_table_name, excel_path, sheet_name, data_range = transform_func(spark)

#     print(f"Reading {sheet_name} from {excel_path}")

#     df = (
#         spark.read
#             .format("com.crealytics.spark.excel")
#             .option("dataAddress", f"'{sheet_name}'!{data_range}")
#             .option("header", "true")
#             .option("treatEmptyValuesAsNulls", "true")
#             .option("inferSchema", "true")
#             .option("addColorColumns", "false")
#             .option("maxRowsInMemory", 10000)
#             .load(excel_path)
#     )

#     print(f"Rows read: {df.count()}")

#     # 1. Clean column names
#     df = clean_column_names(df)

#     # 2. Convert '-' → NULL
#     df = normalize_nulls(df)

#     # 3. Enforce numeric types
#     df = cast_numeric_columns(df)
#     df = df.dropna(how="all")  

#     # -----------------------------------------------------
#     # Write to Iceberg
#     # -----------------------------------------------------
#     if not spark.catalog.tableExists(full_table_name):
#         print(f"Creating Iceberg table {full_table_name}")
#         df.repartition(10).writeTo(full_table_name).create()
#     else:
#         print(f"Overwriting Iceberg table {full_table_name}")
#         df.repartition(10).write.format("iceberg").mode("overwrite").save(full_table_name)

#     print(f"Successfully loaded {full_table_name}")


# # ---------------------------------------------------------
# # Main driver
# # ---------------------------------------------------------
# def transformation_main(spark: SparkSession):
#     # process_table(spark, vert_san_plan_transform_and_load)
#     process_table(spark, vert_ster_plan_transform_and_load)

# from pyspark.sql import SparkSession, functions as F
# from sharepoint_transformation.vert_ster_plan import vert_ster_plan_transform_and_load

# # ---------------------------------------------------------
# # Column name standardization
# # ---------------------------------------------------------
# def clean_column_names(df):
#     """
#     Replace spaces, dots, dashes, and slashes in column names with underscores.
#     """
#     def clean(col_name: str) -> str:
#         return (
#             col_name.strip()
#                     .replace(" ", "_")
#                     .replace(".", "_")
#                     .replace("-", "_")
#                     .replace("/", "_")
#         )
#     return df.select([F.col(c).alias(clean(c)) for c in df.columns])


def _clean_name(n: str) -> str:
    n = (n or "").strip()
    return (
        n.replace(" ", "_")
         .replace(".", "_")
         .replace("-", "_")
         .replace("/", "_")
    )

def _make_unique(names):
    seen = {}
    out = []
    for name in names:
        base = name or ""
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            i = seen[base]
            candidate = f"{base}_{i}"
            while candidate in seen:
                i += 1
                candidate = f"{base}_{i}"
            seen[base] = i + 1
            seen[candidate] = 1
            out.append(candidate)
    return out
# def build_headers_keep_all_rows(df):
#     """
#     Build column headers from the first two rows for period columns, 
#     force first 3 columns as Category/SubCategory/Customer,
#     and **keep all rows** (rows 0, 1, 2, etc.) as data.
#     """
#     df = df.coalesce(1).cache()

#     # Collect first 2 rows for header combination
#     rows = df.limit(2).collect()
#     if len(rows) < 2:
#         raise ValueError("DataFrame must have at least 2 rows")
    
#     row0, row1 = rows

#     new_header = []
#     for idx, c in enumerate(df.columns):
#         if idx == 0:
#             new_header.append("Category")
#         elif idx == 1:
#             new_header.append("SubCategory")
#         elif idx == 2:
#             new_header.append("Customer")
#         else:
#             # Combine Row0 + Row1 for period columns
#             v0 = row0[c]
#             v1 = row1[c]
#             combined = f"{v0}_{v1}" if v0 else str(v1)
#             new_header.append(_clean_name(combined))

#     new_header = _make_unique(new_header)

#     # Keep ALL rows as data
#     df_final = df.toDF(*new_header)
#     return df_final

def combine_headers_keep_row1(df):
    """
    Build column names for SharePoint Excel:
      - First 3 columns: Category, SubCategory, Customer (from row2)
      - Remaining columns: combine row0 + row1 to create period columns
      - Keep row1 as the first data row (do not drop)
    """
    df = df.coalesce(1).cache()

    # Collect first two rows and row2 for fixed column names
    rows = df.limit(3).collect()
    if len(rows) < 3:
        raise ValueError("DataFrame must have at least 3 rows")
    
    row0, row1, row2 = rows

    new_header = []
    for idx, c in enumerate(df.columns):
        if idx == 0:
            new_header.append("Category")
        elif idx == 1:
            new_header.append("SubCategory")
        elif idx == 2:
            new_header.append("Customer")
        else:
            v0 = row0[c]
            v1 = row1[c]
            combined = f"{v0}_{v1}" if v0 else str(v1)
            new_header.append(_clean_name(combined))

    new_header = _make_unique(new_header)

    # Keep **all rows including row1** as data
    df_final = df.toDF(*new_header)
    return df_final

# def combine_first_two_rows_fast_keep_row2(df):
#     """
#     Build new column names from the first two rows:
#       - If row1 value is string: new_name = f"{row1}_{row2}"
#       - Else: new_name = str(row1)
#       - Special-case "0_0" -> "0"
#     Keep original Row 2 as the first data row (drop only Row 1).
#     """
#     df = df.coalesce(1).cache()

#     rows = df.limit(2).collect()
#     if len(rows) < 2:
#         raise ValueError("DataFrame must have at least 2 rows")

#     row1, row2 = rows

#     new_header = []
#     for c in df.columns:
#         v1 = row1[c]
#         v2 = row2[c]
#         if isinstance(v1, str):
#             combined = f"{v1}_{v2}"
#             if combined == "0_0":
#                 combined = "0"
#             name = combined
#         else:
#             name = str(v1) if v1 is not None else ""
#         new_header.append(_clean_name(str(name)))

#     new_header = _make_unique(new_header)

#     # w = Window.orderBy(F.monotonically_increasing_id())
#     # df_rn = df.withColumn("_rn", F.row_number().over(w))
#     # df_data = df_rn.filter(F.col("_rn") >= 2).drop("_rn")
#     df_final = df.toDF(*new_header)
#     return df_final



def normalize_nulls_fast(df):
    """
    Convert Excel placeholders into NULL or 0 efficiently.
    Only applies per-column vectorized operations without casting all to string.
    """
    null_placeholders = ["-", "–", "—", "", " "]
    zero_placeholders = ["- 0","0.0"]

    new_cols = []
    for c, dtype in df.dtypes:
        col_expr = F.col(c)
        if dtype in ("string", "boolean"):
            col_expr = (
                F.when(col_expr.isin(zero_placeholders), 0)
                 .when(col_expr.isin(null_placeholders), 0)
                 .otherwise(col_expr)
            )
        new_cols.append(col_expr.alias(c))

    return df.select(*new_cols)

# -----------------------------
# 3. Clean column names
# -----------------------------
def clean_column_names(df):
    """
    Replace spaces, dots, dashes, and slashes in column names with underscores.
    """
    def clean(col_name: str) -> str:
        return (
            col_name.strip()
                    .replace(" ", "_")
                    .replace(".", "_")
                    .replace("-", "_")
                    .replace("/", "_")
           
        )
    return df.select([F.col(c).alias(clean(c)) for c in df.columns])

# -----------------------------
# 4. Full ETL for a single table
# -----------------------------
def process_table(spark, transform_func):
    _, full_table_name, excel_path, sheet_name, data_range = transform_func(spark)
    print(f"Reading '{sheet_name}' from {excel_path} (range {data_range})")
    header_option = "true" if transform_func is ntiva_lookup_load else "false"
    # Read Excel
    df = (
        spark.read
             .format("com.crealytics.spark.excel")
             .option("dataAddress", f"'{sheet_name}'!{data_range}")
             .option("header", header_option)
             .option("treatEmptyValuesAsNulls", "true")
             .option("inferSchema", "true")
             .option("addColorColumns", "false")
             .option("maxRowsInMemory", 10000)
             .load(excel_path)
    )
    df.printSchema()
    df = clean_column_names(df)
    df = normalize_nulls_fast(df)
    df = df.dropna(how="all")  
    
    if not transform_func == ntiva_lookup_load:
        df = combine_headers_keep_row1(df)

    # Write to Iceberg
    if not spark.catalog.tableExists(full_table_name):
        print(f"Creating Iceberg table {full_table_name}")
        df.writeTo(full_table_name).create()
    else:
        print(f"Overwriting Iceberg table {full_table_name}")
        df.write.format("iceberg").mode("overwrite").save(full_table_name)

    print(f"Successfully loaded '{full_table_name}' with {df.count()} rows.")
    return df


def transformation_main(spark: SparkSession):
    process_table(spark, vert_ster_plan_transform_and_load)
    process_table(spark, vert_san_plan_transform_and_load)
    process_table(spark, vert_alex_plan_transform_and_load)
    process_table(spark,ntiva_lookup_load)