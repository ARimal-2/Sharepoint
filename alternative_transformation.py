from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from pyspark.sql.types import DoubleType, StringType, DateType
import datetime
import re
from sharepoint_transformation.vert_san_plan import vert_san_plan_transform_and_load
from sharepoint_transformation.vert_alex_plan import vert_alex_plan_transform_and_load
from sharepoint_transformation.vert_ster_plan import vert_ster_plan_transform_and_load
from sharepoint_transformation.ntiva_lookup import ntiva_lookup_load
from Sharepoint_transformation_2025.vert_ster_2025 import vert_ster_25_plan_transform_and_load
from Sharepoint_transformation_2025.vert_san_plan_2025 import vert_san_25_plan_transform_and_load
from sharepoint_transformation.forecast_san import vert_fore_san_transform_and_load
from sharepoint_transformation.forecast_ster import vert_fore_ster_transform_and_load
from Sharepoint_transformation_2025.forecast_san import vert_fore_san_25_transform_and_load
from Sharepoint_transformation_2025.forecast_ster import vert_fore_ster_25_transform_and_load
from forecast_transformation import forecast_transform

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

def transform_plan_transposed(df: DataFrame, city: str, state: str, week_column: str) -> DataFrame:
    """
    Refactored distributed transformation .
    Injects metadata as literals to minimize Spark plan complexity and prevent Driver OOM.
    """
    cols = df.columns
    n_cols = len(cols)

    # This removes the metadata handling from the Spark logical plan entirely.
    period_row = df.filter(F.lower(F.trim(F.col(cols[0]))) == "period").first()
    date_row = df.filter(F.lower(F.trim(F.col(cols[0]))) == "date").first()
    
    # Fallback: if date_row not found, use week_column row
    if date_row is None:
        date_row = df.filter(F.lower(F.trim(F.col(cols[0]))) == week_column.lower()).first()

    if not period_row or not date_row:
        raise ValueError("Missing 'Period' label or 'Date'/'Week' labels in column 0")

    # 2. Build literal metadata lists
    p_lits = []
    d_lits = []
    for i in range(1, n_cols):
        print (f"period_row[{i}]={period_row[i]}")
        p_val = str(period_row[i]).strip() if period_row[i] is not None else "Unknown"
        print(p_val)
        d_val = str(date_row[i]).strip() if date_row[i] is not None else None
        p_lits.append(F.lit(p_val))
        d_lits.append(F.lit(d_val))

    # 3. Filter data rows (SKUs)
    data_df = df.filter(~F.lower(F.trim(F.col(cols[0]))).isin("period", week_column.lower(), "date")) \
                .filter(F.col(cols[0]).isNotNull())

    # 4. Melt using zip/explode with LITERAL metadata
    # This is much lighter than cross-joining dataframes
    val_cols = [F.col(cols[i]) for i in range(1, n_cols)]
    
    df_long = data_df.withColumn("zipped", F.explode(F.arrays_zip(
        F.array(*p_lits).alias("p"),
        F.array(*d_lits).alias("d"),
        F.array(*val_cols).alias("v")
    )))

    raw_date = F.coalesce(
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
    )

    df_final = df_long.select(
        # SKU Cleaning
        F.when(
            F.col(cols[0]).cast("double").isNotNull(),
            F.regexp_replace(F.col(cols[0]).cast("string"), "\\.0$", "")
        ).otherwise(F.trim(F.col(cols[0]))).alias("Item"),

        F.col("zipped.p").alias("Period"),

        F.when(F.year(raw_date) < 100, F.add_months(raw_date, 2000 * 12)).otherwise(raw_date).alias("Date"),

        # Value cleaning
        F.when(
            F.regexp_replace(F.trim(F.col("zipped.v").cast("string")), "[^0-9.\\-]", "").isin("", "-"),
            None
        ).otherwise(
            F.regexp_replace(F.col("zipped.v").cast("string"), "[^0-9.\\-]", "").cast(DoubleType())
        ).alias("Planning_Value")
    )

    # 6. Add Context and Filter
    df_final = df_final.withColumn("City", F.lit(city)) \
                       .withColumn("State", F.lit(state)) \
                       .filter(
                           (F.col("Item").cast("string") != "0") & 
                           (F.col("Item").cast("string") != "0.0") & 
                           (F.col("Item").isNotNull())
                       )
    
    return df_final

def process_table(spark: SparkSession, transform_func):
    config_result = transform_func(spark)
    # Correctly unpack based on the number of values returned
    if len(config_result) == 9:
        _, full_table_name, excel_path, sheet_name, data_range, week_column, city, state, pdwks_range = config_result
    else:
        _, full_table_name, excel_path, sheet_name, data_range, week_column, city, state = config_result
        pdwks_range = None

    print(f"Reading {sheet_name} from {excel_path} (range {data_range})")
    header_option = "true" if transform_func is ntiva_lookup_load else "false"
    df = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", f"'{sheet_name}'!{data_range}")
        .option("header", header_option)
        .option("inferSchema", "false")
        .option("usePlainNumberFormat", "true")
        .option("useStreamingReader", "true")
        .option("maxRowsInMemory", "50")
        .option("treatEmptyValuesAsNulls", "true")
        .load(excel_path)
        .dropna(how="all")
    )
    
    forecast_funcs = (
        vert_fore_san_transform_and_load, 
        vert_fore_ster_transform_and_load,
        vert_fore_san_25_transform_and_load,
        vert_fore_ster_25_transform_and_load
    )
    
    if transform_func in forecast_funcs:
        df = forecast_transform(spark, df, city, excel_path, pdwks_range)
    elif transform_func != ntiva_lookup_load:
        df = transform_plan_transposed(df, city, state, week_column)
    else:
        df = df.withColumn(
            "item",
            F.col("item").cast("double").cast("long")
        )
        
    # Write to Iceberg
    print(f"Sample of data for {full_table_name}:")
    df.show(5, truncate=False)
    
    if transform_func in forecast_funcs:
        print(f"Replacing Iceberg table {full_table_name} to match new schema")
        df.writeTo(full_table_name).createOrReplace()
    elif not spark.catalog.tableExists(full_table_name):
        print(f"Creating Iceberg table {full_table_name}")
        df.writeTo(full_table_name).create()
    else:
        print(f"Overwriting Iceberg table {full_table_name}")
        df.write.format("iceberg").mode("overwrite").save(full_table_name)

    print(f"Loaded {df.count()} rows into {full_table_name}")
    df.limit(5).show()
    return df

# -----------------------------
# Main transformation entry
# -----------------------------

def transformation_main(spark: SparkSession):
    # 2026
    # process_table(spark, vert_san_plan_transform_and_load)
    # process_table(spark, vert_alex_plan_transform_and_load)
    # process_table(spark, vert_ster_plan_transform_and_load)

    # #2026 forecast
    process_table(spark, vert_fore_san_transform_and_load)
    process_table(spark, vert_fore_ster_transform_and_load)
    
    # #2025 forecast
    process_table(spark, vert_fore_san_25_transform_and_load)
    process_table(spark, vert_fore_ster_25_transform_and_load)
    
    #lookup table
    # process_table(spark, ntiva_lookup_load)

    # # # 2025
    # process_table(spark, vert_ster_25_plan_transform_and_load)
    # process_table(spark,vert_san_25_plan_transform_and_load)
