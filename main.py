from pyspark.sql import SparkSession
from transformation import transform_and_load

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("sharepoint")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Run the transformation
    transform_and_load(spark)

    # Stop Spark session
    spark.stop()
