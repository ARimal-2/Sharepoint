# import ibm_boto3
# from ibm_botocore.client import Config
# from ibm_botocore.exceptions import ClientError
# import os
# from dotenv import load_dotenv

# load_dotenv()

# api_key = os.getenv("API_KEY")
# service_instance_id = os.getenv("SERVICE_INSTANCE_ID")
# endpoint_url = os.getenv("IBM_COS_ENDPOINT")
# bucket_name = "sample-testing-data"
# prefix = "Sharepoint/"  # note the trailing slash

# cos = ibm_boto3.client(
#     "s3",
#     ibm_api_key_id=api_key,
#     ibm_service_instance_id=service_instance_id,
#     config=Config(signature_version="oauth"),
#     endpoint_url=endpoint_url
# )

# # List all objects under the prefix
# try:
#     response = cos.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
#     if 'Contents' in response:
#         objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
#         print(f"Deleting {len(objects_to_delete)} objects under '{prefix}'...")
#         delete_response = cos.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
#         print("Deleted successfully:", delete_response)
#     else:
#         print(f"No objects found under prefix '{prefix}'")
# except ClientError as e:
#     print(f"Error: {e}")
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from sharepoint_transformation.vert_san_plan import vert_san_plan_transform_and_load

# -----------------------------
# Utility functions
# -----------------------------

def clean_name(name: str) -> str:
    return (
        (name or "")
        .strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("/", "_")
    )

def make_unique(names):
    counts = {}
    result = []
    for n in names:
        base = clean_name(n)
        if counts.get(base, 0) > 0:
            result.append(f"{base}_{counts[base]}")
        else:
            result.append(base)
        counts[base] = counts.get(base, 0) + 1
    return result

# -----------------------------
# Core transformation
# -----------------------------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def transform_plan_transposed(df: DataFrame, city: str, state: str) -> DataFrame:
    """
    Refactored distributed transformation (V4).
    Injects metadata as literals to minimize Spark plan complexity and prevent Driver OOM.
    """
    cols = df.columns
    n_cols = len(cols)
    
    # DIAGNOSTIC VERSION MARKER
    print("-" * 50)
    print(f"DIAGNOSTIC VERSION: 2026-01-22-V4")
    print(f"DIAGNOSTIC: n_cols = {n_cols}")
    print("-" * 50)

    # 1. Extract Metadata to driver (Safe: only 2 rows, ~700 fields)
    # This removes the metadata handling from the Spark logical plan entirely.
    period_row = df.filter(F.lower(F.trim(F.col(cols[0]))) == "period").first()
    date_row = df.filter(F.lower(F.trim(F.col(cols[0]))) == "date").first()

    if not period_row or not date_row:
        raise ValueError("Missing 'Period' or 'Date' labels in column 0")

    # 2. Build literal metadata lists
    p_lits = []
    d_lits = []
    for i in range(1, n_cols):
        p_val = str(period_row[i]).strip().replace("-", "_") if period_row[i] is not None else "Unknown"
        d_val = str(date_row[i]).strip() if date_row[i] is not None else None
        p_lits.append(F.lit(p_val))
        d_lits.append(F.lit(d_val))

    # 3. Filter data rows (SKUs)
    data_df = df.filter(~F.lower(F.trim(F.col(cols[0]))).isin("period", "dow", "date")) \
                .filter(F.col(cols[0]).isNotNull())

    # 4. Melt using zip/explode with LITERAL metadata
    # This is much lighter than cross-joining dataframes
    val_cols = [F.col(cols[i]) for i in range(1, n_cols)]
    
    df_long = data_df.withColumn("zipped", F.explode(F.arrays_zip(
        F.array(*p_lits).alias("p"),
        F.array(*d_lits).alias("d"),
        F.array(*val_cols).alias("v")
    )))

    # 5. Parse and Clean Final Columns
    df_final = df_long.select(
        # SKU Cleaning
        F.when(
            F.col(cols[0]).cast("double").isNotNull(),
            F.regexp_replace(F.col(cols[0]).cast("string"), "\\.0$", "")
        ).otherwise(F.trim(F.col(cols[0]))).alias("sku"),

        F.col("zipped.p").alias("period"),

        # Robust Date Parsing
        F.coalesce(
            # Excel Serial Date fallback
            F.when(
                F.col("zipped.d").cast("double").isNotNull(),
                F.date_add(F.to_date(F.lit("1899-12-30")), F.col("zipped.d").cast("int"))
            ),
            F.to_date(F.col("zipped.d"), "M/d/yyyy"),
            F.to_date(F.col("zipped.d"), "MM/dd/yyyy"),
            F.to_date(F.col("zipped.d"), "M/d/yy"),
            F.to_date(F.col("zipped.d"), "MM/dd/yy"),
            F.to_date(F.col("zipped.d"), "yyyy-MM-dd"),
            F.to_date(F.col("zipped.d")),
            F.to_date(F.to_timestamp(F.col("zipped.d")))
        ).alias("date"),

        # Value cleaning
        F.when(
            F.regexp_replace(F.trim(F.col("zipped.v").cast("string")), "[^0-9.\\-]", "").isin("", "-"),
            None
        ).otherwise(
            F.regexp_replace(F.col("zipped.v").cast("string"), "[^0-9.\\-]", "").cast(DoubleType())
        ).alias("value")
    )

    # 6. Add Context
    df_final = df_final.withColumn("city", F.lit(city)) \
                       .withColumn("state", F.lit(state))

    return df_final

def process_table(spark: SparkSession, transform_func):
    _, full_table_name, excel_path, sheet_name, data_range, week_column, city, state = transform_func(spark)

    print(f"Reading {sheet_name} from {excel_path} (range {data_range})")

    df = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", f"'{sheet_name}'!{data_range}")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("usePlainNumberFormat", "true")
        .option("useStreamingReader", "true")
        .option("maxRowsInMemory", "50")
        .option("treatEmptyValuesAsNulls", "true")
        .load(excel_path)
        .dropna(how="all")
    )

    df_final = transform_plan_transposed(df, city, state)

    # Write to Iceberg
    if not spark.catalog.tableExists(full_table_name):
        print(f"Creating Iceberg table {full_table_name}")
        df_final.writeTo(full_table_name).create()
    else:
        print(f"Overwriting Iceberg table {full_table_name}")
        df_final.write.format("iceberg").mode("overwrite").save(full_table_name)

    print(f"Loaded {df_final.count()} rows into {full_table_name}")
    df_final.limit(5).show()
    return df_final

# -----------------------------
# Main transformation entry
# -----------------------------

def transformation_main(spark: SparkSession):
    process_table(spark, vert_san_plan_transform_and_load)
