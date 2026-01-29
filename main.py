from pyspark.sql import SparkSession
from alternative_transformation import transformation_main
from excel_file_read import excel_main
from fact_planning_transformation import build_fact_planning_table
from fact_forecast_transformation import build_fact_forecast_table

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("sharepoint")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # 1. Check if Excel files were updated
    file_updated = excel_main()
    
    if file_updated:
        print("Changes detected. Starting processing sequence...")
        
        # 2. Run individual table transformations
        transformation_main(spark)
        
        # 3. Build the combined fact tables
        print("Starting Fact Planning Table Build...")
        build_fact_planning_table(spark)

        print("Starting Fact Forecast Table Build...")
        build_fact_forecast_table(spark)
    
        print("Processing sequence completed successfully.")
    else:
        print("No changes detected. Skipping transformation and fact table build.")

    # Stop Spark session
    spark.stop()
