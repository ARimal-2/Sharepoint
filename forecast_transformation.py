
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

from pyspark.sql import DataFrame, functions as F, Window
import datetime

def forecast_transform(df: DataFrame) -> DataFrame:
    """
    Output Schema:
    Period | Date | SKU | Item | Forecast_Value
    """

    cols = df.columns
    if not cols:
        return df

    period_col = cols[0]

    # ---------------------------------------------------------
    # 1. Locate SKU header row
    # ---------------------------------------------------------
    preview_rows = df.limit(25).collect()
    if not preview_rows:
        return df

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

    # ---------------------------------------------------------
    # 2. Detect first forecast period row dynamically
    # ---------------------------------------------------------
    data_start_idx = None
    for idx in range(sku_row_idx + 1, len(preview_rows)):
        val = str(preview_rows[idx][0]).strip()
        if val.startswith("F") and "-" in val:
            data_start_idx = idx
            break

    if data_start_idx is None:
        raise ValueError("Could not detect forecast period rows.")

    # ---------------------------------------------------------
    # 3. Build column mappings (include ALL item/SKU fields)
    # ---------------------------------------------------------
    item_lits, sku_lits, val_cols = [], [], []

    for i in range(1, len(cols)):
        item_val = item_row[i]
        sku_val = sku_row[i]

        if (item_val is None or str(item_val).strip() == "") and \
           (sku_val is None or str(sku_val).strip() == ""):
            continue

        item_str = str(item_val).strip() if item_val not in (None, "") else None
        sku_str = str(sku_val).strip() if sku_val not in (None, "") else None

        item_lits.append(F.lit(item_str))
        sku_lits.append(F.lit(sku_str))
        val_cols.append(F.col(cols[i]))

    # ---------------------------------------------------------
    # 4. Add proper sequential row index
    # ---------------------------------------------------------
    w = Window.orderBy(F.monotonically_increasing_id())
    df_with_id = df.withColumn("_row_id", F.row_number().over(w) - 1)

    data_df = (
        df_with_id
        .filter(F.col("_row_id") >= data_start_idx)
        .drop("_row_id")
    )

    # ---------------------------------------------------------
    # 5. Melt wide → long
    # ---------------------------------------------------------
    df_long = data_df.withColumn(
        "zipped",
        F.explode(
            F.arrays_zip(
                F.array(*item_lits).alias("item"),
                F.array(*sku_lits).alias("sku"),
                F.array(*val_cols).alias("qty")
            )
        )
    )

    res = (
        df_long.select(
            F.trim(F.col(period_col)).alias("Period"),
            F.col("zipped.sku").alias("SKU_raw"),
            F.col("zipped.item").alias("Item_raw"),
            F.col("zipped.qty").alias("Forecast_raw")
        )
        .filter(
            F.col("Period").isNotNull() & 
            (F.col("Period") != "") &
            ~F.lower(F.col("Period")).contains("pallet") &
            ~F.lower(F.col("Period")).contains("weight") &
            ~F.lower(F.col("Period")).contains("cases") &
            ~F.col("Period").rlike("^[0-9.]+$")
        )
    )

    # ---------------------------------------------------------
    # 6. Type handling: Keep Item as in Excel, clean SKU, round Forecast
    # ---------------------------------------------------------
    res = res.withColumn(
        "Item",
        F.col("Item_raw").cast("double"),  # keep exactly as in Excel (nulls stay null)
    ).withColumn(
        "SKU",  
        F.when(F.col("SKU_raw").rlike("^[0-9]+(?:\\.0)?$"),
               F.col("SKU_raw").cast("int"))  # remove .0 if numeric
         .otherwise(F.col("SKU_raw"))  # keep alphanumeric intact
    ).withColumn(
        "Forecast_Value",
        F.round(
            F.regexp_replace(F.col("Forecast_raw"), "[^0-9.]", "").cast("double"), 
            0
        )
    ).withColumn(
        "Forecast_Value",
        F.coalesce(F.col("Forecast_Value"), F.lit(0.0))
    )

    # ---------------------------------------------------------
    # 7. Period → Date mapping
    # ---------------------------------------------------------
    ordered_periods = (
        res.select("Period")
           .distinct()
           .rdd.map(lambda r: r[0])
           .collect()
    )

    start_date = datetime.date(2025, 6, 30)
    mapping_data = [
        (p, start_date + datetime.timedelta(days=7 * i))
        for i, p in enumerate(ordered_periods)
    ]

    spark = df.sparkSession
    mapping_df = spark.createDataFrame(mapping_data, ["Period", "Date"])


    output_cols = ["Period", "Date", "SKU", "Item", "Forecast_Value"]
    

    item_is_null = res.filter(F.col("Item").isNotNull()).count() == 0
    if item_is_null:
        output_cols.remove("Item")

    final_df = (
        res.join(mapping_df, on="Period", how="left")
           .select(*output_cols)
    )

    return final_df
