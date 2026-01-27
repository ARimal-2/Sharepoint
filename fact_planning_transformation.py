from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from config import get_config
import re

def build_fact_planning_table(spark: SparkSession):
    """
    Builds the final fact_planning_table by combining individual planning tables.
    Matches the schema shown in the user's report:
    PrimaryKey | PDIL | Period | Date | Item | Planning_Value | Location | City | State
    """
    catalog_name = get_config("catalog_name", required=True)
    db_name = get_config("db_name", required=True)
    target_table = f"{catalog_name}.{db_name}.fact_planning_table"

    # 1. Define source table names
    sources = [
        {"table": f"{catalog_name}.{db_name}.Vert_SAN_Plan_FY2025"},
        {"table": f"{catalog_name}.{db_name}.Vert_STER_Plan_FY2025"},
        {"table": f"{catalog_name}.{db_name}.Vert_SAN_Plan_FY2026"},
        {"table": f"{catalog_name}.{db_name}.Vert_STER_Plan_FY2026"},
        {"table": f"{catalog_name}.{db_name}.Vert_ALEX_Plan_FY2026"}
    ]

    combined_dfs = []
    
    print("Loading and mapping source tables to match target schema...")
    for s in sources:
        try:
            full_table_path = s['table']
            df = spark.table(full_table_path)
            
            # Infer Location from table name
            table_name_only = full_table_path.split('.')[-1]
            source_match = re.search(r'Vert_([A-Z]+)_Plan', table_name_only)
            location_val = source_match.group(1) if source_match else "Unknown"

            # Map source columns to target schema
            # Determine which value column to use
            if "Planning_Value" in df.columns:
                value_col = F.col("Planning_Value")
            elif "Forecast_Value" in df.columns:
                value_col = F.col("Forecast_Value")
            else:
                raise ValueError(f"Neither 'Planning_Value' nor 'Forecast_Value' found in {full_table_path}")
            
            # Handle missing City/State columns (forecast tables don't have them)
            city_col = F.col("City") if "City" in df.columns else F.lit(None).cast("string")
            state_col = F.col("State") if "State" in df.columns else F.lit(None).cast("string")
            
            df_mapped = df.select(
                F.col("Period"),
                F.col("Date"),
                F.col("Item"),
                value_col.alias("Planning_Value"),
                city_col.alias("City"),
                state_col.alias("State"),
                F.lit(location_val).alias("Location")
            )
            combined_dfs.append(df_mapped)
            print(f"Successfully mapped {full_table_path}")
        except Exception as e:
            print(f"Warning: Skipping source {s['table']}. Error: {e}")

    if not combined_dfs:
        print("No source data found. Exiting fact table build.")
        return

    # 2. Union all sources
    combined_planning_df = combined_dfs[0]
    for df in combined_dfs[1:]:
        combined_planning_df = combined_planning_df.unionByName(df)
    
    # Diagnostic: Count rows after union
    total_rows_after_union = combined_planning_df.count()
    print(f"Total rows after union: {total_rows_after_union:,}")

    # 3. Add PDIL and PrimaryKey columns
    # PDIL format: Period-Date-Item-Location
    df_with_keys = (
        combined_planning_df
        .withColumn("PDIL", F.concat_ws("-", 
            F.col("Period"), 
            F.date_format(F.col("Date"), "yyyy-MM-dd"), 
            F.col("Item"), 
            F.col("Location")
        ))
    )

    # 4. Deduplicate using PDIL as the unique key
    # Note: Using the filter to keep only 1 row per unique Period-Date-Item-Location.
    window_spec = Window.partitionBy("PDIL").orderBy(F.lit(1))
    dedup_df = (
        df_with_keys
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)  # Re-enabled to remove duplicates
        .drop("_row_num")
    )
    
    # Diagnostic: Confirm matching counts
    total_rows_final = dedup_df.count()
    print(f"Total rows in fact transformation: {total_rows_final:,}")
    if total_rows_final == total_rows_after_union:
        print("✅ SUCCESS: All source rows are included (Deduplication disabled)")
    else:
        print(f"⚠️  Note: Row count changed from {total_rows_after_union:,} to {total_rows_final:,}")

    # 5. Handle Target Table Logic (Initialization or Merge)
    if not spark.catalog.tableExists(target_table):
        print(f"Initializing fact_planning_table: {target_table}")
        
        # Add PrimaryKey for the first load
        final_init_df = (
            dedup_df
            .withColumn("PrimaryKey", F.row_number().over(Window.orderBy("PDIL")))
            .select("PrimaryKey", "PDIL", "Period", "Date", "Item", "Planning_Value", "Location", "City", "State")
        )
        
        (
            final_init_df
            .writeTo(target_table)
            .tableProperty("format-version", "2")
            .create()
        )
        print("Target table created successfully.")
        return

    # 6. Incremental MERGE INTO logic
    print(f"Merging updates into {target_table}...")
    dedup_df.createOrReplaceTempView("dedup_source_view")

    # Regular Update for changed values 
    # Logic: We use a CTE to ensure the source is unique for the 'ON' condition to avoid Cardinality errors.
    try:
        spark.sql(f"""
            WITH unique_source AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY PDIL ORDER BY 1) as rn
                    FROM dedup_source_view
                ) WHERE rn = 1
            )
            MERGE INTO {target_table} AS t
            USING unique_source AS s
            ON t.PDIL = s.PDIL
            WHEN MATCHED AND (t.Planning_Value != s.Planning_Value OR s.Planning_Value IS NOT NULL AND t.Planning_Value IS NULL) THEN
                UPDATE SET 
                    t.Planning_Value = s.Planning_Value,
                    t.Item = s.Item
        """)
        
        # Handle New Inserts separately to manage PrimaryKey generation reliably
        # This will include ALL rows from the source that don't have a matching PDIL in the target,
        # including duplicates within the source itself.
        max_id = spark.sql(f"SELECT COALESCE(MAX(PrimaryKey), 0) FROM {target_table}").collect()[0][0]
        
        new_records = (
            spark.sql(f"""
                SELECT s.* 
                FROM dedup_source_view s 
                LEFT JOIN {target_table} t ON s.PDIL = t.PDIL 
                WHERE t.PDIL IS NULL
            """)
            .withColumn("PrimaryKey", F.row_number().over(Window.orderBy("PDIL")) + max_id)
            .select("PrimaryKey", "PDIL", "Period", "Date", "Item", "Planning_Value", "Location", "City", "State")
        )
        
        if new_records.count() > 0:
            print(f"Inserting {new_records.count()} new records...")
            new_records.write.format("iceberg").mode("append").save(target_table)
            
    except Exception as e:
        print(f"Error during merge/insert: {e}")
        raise e
        
    print(f"Process completed for {target_table}")
