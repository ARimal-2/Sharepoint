from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from config import get_config
import re

def build_fact_forecast_table(spark: SparkSession):
    """
    Builds the final fact_forecast_table by combining individual forecast tables.
    Schema: IndexKey | PDIL | Period | Date | SKU | Item | Forecast_Value | Location
    """
    catalog_name = get_config("catalog_name", required=True)
    db_name = get_config("db_name", required=True)
    target_table = f"{catalog_name}.{db_name}.Fact_Forecast_Data"

    # 1. Define source forecast table names
    sources = [
        {"table": f"{catalog_name}.{db_name}.Forecast_SAN_FY2025"},
        {"table": f"{catalog_name}.{db_name}.Forecast_STER_FY2025"},
        {"table": f"{catalog_name}.{db_name}.Forecast_SAN_FY2026"},
        {"table": f"{catalog_name}.{db_name}.Forecast_STER_FY2026"}
    ]

    combined_dfs = []
    
    print("Loading and mapping source forecast tables to match target schema...")
    for s in sources:
        try:
            full_table_path = s['table']
            if not spark.catalog.tableExists(full_table_path):
                print(f"Skipping {full_table_path} because it does not exist.")
                continue
                
            df = spark.table(full_table_path)
            
            # Infer Location from table name (e.g. Forecast_SAN_FY2026 -> SAN)
            table_name_only = full_table_path.split('.')[-1]
            source_match = re.search(r'Forecast_([A-Z]+)_', table_name_only)
            location_val = source_match.group(1).upper() if source_match else "Unknown"

            # Map source columns to target schema
            df_mapped = df.select(
                F.col("Period"),
                F.col("Date"),
                F.col("SKU"),
                F.col("Item"),
                F.col("Forecast_Value"),
                F.lit(location_val).alias("Location")
            )
            combined_dfs.append(df_mapped)
            print(f"Successfully mapped {full_table_path}")
        except Exception as e:
            print(f"Warning: Skipping source {s['table']}. Error: {e}")

    if not combined_dfs:
        print("No source forecast data found. Exiting fact forecast table build.")
        return

    # 2. Union all sources
    combined_forecast_df = combined_dfs[0]
    for df in combined_dfs[1:]:
        combined_forecast_df = combined_forecast_df.unionByName(df)
    
    # 3. Add PDIL (Period-Date-Item-SKU-Location) to identify unique records
    df_with_keys = (
        combined_forecast_df
        .withColumn("PDIL", F.concat_ws("-", 
            F.col("Period"), 
            F.date_format(F.col("Date"), "yyyy-MM-dd"), 
            F.coalesce(F.col("Item").cast("string"), F.lit("NA")),
            F.col("Location")
        ))
    )


    # 5. Handle Target Table Logic
    if not spark.catalog.tableExists(target_table):
        print(f"Initializing fact_forecast_table: {target_table}")
        
        final_init_df = (
            df_with_keys
            .withColumn("IndexKey", F.row_number().over(Window.orderBy("PDIL")))
            .select("IndexKey", "PDIL", "Period", "Date", "SKU", "Item", "Forecast_Value", "Location")
        )
        
        final_init_df.writeTo(target_table).tableProperty("format-version", "2").create()
        print("Target fact_forecast_table created successfully.")
    else:
        print(f"Merging updates into {target_table}...")
        df_with_keys.createOrReplaceTempView("dedup_forecast_source_view")

        # Merge Logic (Incremental)
        try:

            max_id = spark.sql(f"SELECT COALESCE(MAX(IndexKey), 0) FROM {target_table}").collect()[0][0]

            # 1. DELETE existing records that match the incoming PDILs
            print("Deleting existing records matching incoming PDILs to handle potential updates/duplicates...")
            spark.sql(f"""
                DELETE FROM {target_table}
                WHERE PDIL IN (SELECT PDIL FROM dedup_forecast_source_view)
            """)
            
            # 2. INSERT all source records (including duplicates)
            print(f"Inserting source records (preserving any duplicates)...")
            
            to_insert = (
                df_with_keys
                .withColumn("IndexKey", F.row_number().over(Window.orderBy("PDIL")) + max_id)
                .select("IndexKey", "PDIL", "Period", "Date", "SKU", "Item", "Forecast_Value", "Location")
            )
            
            to_insert.write.format("iceberg").mode("append").save(target_table)

                
        except Exception as e:
            print(f"Error during forecast merge/insert: {e}")
            raise e
            
    print(f"Process completed for {target_table}")
