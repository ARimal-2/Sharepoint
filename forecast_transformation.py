from pyspark.sql import SparkSession, DataFrame, functions as F, Window
import datetime
import re

def forecast_transform(spark: SparkSession, df: DataFrame, city: str, excel_path: str, pdwks_range: str = "A2557:B2920") -> DataFrame:
    """
    Output Schema:
    Period | Date | SKU | Item | Forecast_Value
    """
    if pdwks_range is None:
        pdwks_range = "A2557:B2920"

    cols = df.columns
    if not cols:
        return df

    period_col = cols[0]

    # 1. Locate SKU header row
    preview_rows = df.limit(25).collect()
    sku_row_idx = -1
    for idx, row in enumerate(preview_rows):
        first_val = str(row[0]).strip().lower() if row[0] else ""
        if first_val == "sku":
            sku_row_idx = idx
            break

    if sku_row_idx == -1:
        raise ValueError("SKU header row not found.")

    sku_row = preview_rows[sku_row_idx]
    item_row = preview_rows[sku_row_idx - 1]

    # 2. Detect first forecast period row dynamically
    data_start_idx = None
    for idx in range(sku_row_idx + 1, len(preview_rows)):
        val = str(preview_rows[idx][0]).strip()
        if val.startswith("F") and "-" in val:
            data_start_idx = idx
            break

    if data_start_idx is None:
        raise ValueError("Could not detect forecast period rows.")

    # 3. Build column mappings
    item_lits, sku_lits, val_cols = [], [], []
    for i in range(1, len(cols)):
        item_val, sku_val = item_row[i], sku_row[i]
        if (item_val is None or str(item_val).strip() == "") and (sku_val is None or str(sku_val).strip() == ""):
            continue
        item_str = str(item_val).strip() if item_val not in (None, "") else None
        sku_str = str(sku_val).strip() if sku_val not in (None, "") else None
        item_lits.append(F.lit(item_str))
        sku_lits.append(F.lit(sku_str))
        val_cols.append(F.col(cols[i]))

    # 4. Filter data
    w = Window.orderBy(F.monotonically_increasing_id())
    data_df = df.withColumn("_row_id", F.row_number().over(w) - 1).filter(F.col("_row_id") >= data_start_idx)

    # 5. Melt wide -> long
    df_long = data_df.withColumn("zipped", F.explode(F.arrays_zip(
        F.array(*item_lits).alias("item"),
        F.array(*sku_lits).alias("sku"),
        F.array(*val_cols).alias("qty")
    )))

    res = (
        df_long.select(
            F.trim(F.col(period_col)).alias("Period"),
            F.col("zipped.sku").alias("SKU_raw"),
            F.col("zipped.item").alias("Item_raw"),
            F.col("zipped.qty").alias("Forecast_raw")
        )
        .filter(F.col("Period").isNotNull() & (F.col("Period") != "") & 
                ~F.lower(F.col("Period")).rlike("pallet|weight|cases") & ~F.col("Period").rlike("^[0-9.]+$"))
    )

    # 6. Type handling
    # 6. Type handling
    # Changed Item to String to support alphanumeric combinations (e.g. 600 + Varchar SKU)
    res = res.withColumn("Item", F.when(F.col("Item_raw").rlike("^[0-9]+(?:\\.0)?$"), 
                                      F.regexp_replace(F.col("Item_raw"), "\\.0$", ""))
                               .otherwise(F.col("Item_raw"))) \
             .withColumn("SKU", F.when(F.col("SKU_raw").rlike("^[0-9]+(?:\\.0)?$"), F.regexp_replace(F.col("SKU_raw"), "\\.0$", "")).otherwise(F.col("SKU_raw"))) \
             .withColumn("Forecast_Value", F.round(F.regexp_replace(F.col("Forecast_raw"), "[^0-9.]", "").cast("double"), 0)) \
             .withColumn("Forecast_Value", F.coalesce(F.col("Forecast_Value"), F.lit(0.0)))

    if city in ["SAN", "San Antonio"]:
        # Removed .cast("double") to allow alphanumeric Item codes
        res = res.withColumn("Item", F.when(F.col("Item").isNull() | (F.trim(F.col("Item")) == ""), F.concat(F.lit("600"), F.col("SKU"))).otherwise(F.col("Item")))
    elif city in ["STER", "Sterling"]:
        # Removed .cast("double") to allow alphanumeric Item codes
        res = res.withColumn("Item", F.when(F.col("Item").isNull() | (F.trim(F.col("Item")) == ""), F.concat(F.lit("200"), F.col("SKU"))).otherwise(F.col("Item")))

    # 7. Read Period-to-Date mapping from PdWks (Range A2557:B2920)
    print(f"Reading PdWks mapping from {excel_path}...") 
    pdwks_df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("dataAddress", f"'PdWks'!{pdwks_range}")
        .option("header", "false")
        .option("useStreamingReader", "true")
        .option("maxRowsInMemory", "50")
        .load(excel_path)
    )
    
    # Define columns: A=Date, B=Period. Filter for Mondays (Day 2 in Spark, Sunday=1)
    # Using dayofweek(date) where 1=Sunday, 2=Monday...
    mapping_df = (
        pdwks_df.select(
            F.col("_c0").alias("raw_date"),
            F.trim(F.col("_c1")).alias("Period")
        )
        .withColumn("raw_date_parsed", F.coalesce(
            F.to_date(F.col("raw_date"), "M/d/yyyy"),
            F.to_date(F.col("raw_date"), "M/d/yy"),
            F.when(F.col("raw_date").cast("double").isNotNull(), 
                   F.date_add(F.to_date(F.lit("1899-12-30")), F.col("raw_date").cast("int")))
        ))
        .withColumn("Date", 
            F.when(F.year(F.col("raw_date_parsed")) < 100, 
                   F.add_months(F.col("raw_date_parsed"), 2000 * 12))
             .otherwise(F.col("raw_date_parsed"))
        )
        .filter(F.dayofweek(F.col("Date")) == 2) # Keep only Mondays
        .select("Period", "Date")
        .distinct()
    )

    final_df = (
        res.join(mapping_df, on="Period", how="left")
           .select("Period", "Date", "SKU", "Item", "Forecast_Value")
    )

    return final_df
