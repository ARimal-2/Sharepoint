from pyspark.sql import SparkSession
from alternative_transformation import transformation_main
from excel_file_read import excel_main

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("sharepoint")
        .enableHiveSupport()
        .getOrCreate()
    )
    

    
    # file_updated = excel_main()
    transformation_main(spark)
    # if file_updated:
        
    # else:
    #     print("Skipping transformation.")

    # Stop Spark session
    spark.stop()
