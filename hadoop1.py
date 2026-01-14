import os
import base64
import traceback
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col
from s3uploader.connect import connect_to_cos
# Environment
os.environ["PYSPARK_ACCUMULATORS_ENABLED"] = "false"

# Watsonx / HMS Credentials
wxd_username = "ankit.rimal@crushbank.com"
wxd_apikey = "1WGn3ZlQabGETxvCRwX8Ru13IUPUv2cRCQ0GT-G2aobVE"

wxd_hms_username = f"ibmlhapikey_{wxd_username}"
wxd_hms_password = wxd_apikey

wxd_encoded_apikey = "Basic " + base64.b64encode(
    f"{wxd_hms_username}:{wxd_hms_password}".encode("utf-8")
).decode("utf-8")

# COS Credentials
cos_endpoint = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
cos_bucket = "sample-testing-data" 
cos_access_key = "6de1127078464ff6bb1c15935efd95c6"
cos_secret_key = "0cf9f4f337ebf747934c71d2f7e4c24e6cbefd4e64faa847"

# Excel path in COS
excel_path = (
    f"s3a://{cos_bucket}/Sharepoint/FY26 Master Planning File/2026/01/05/"
    "20260105T131937_FY26 Master Planning File.xlsx"
)

# Spark Configuration
conf = SparkConf()

conf.setAll([
    ("spark.rpc.message.maxSize", "1024"),
    ("spark.rpc.netty.maxFrameSize", "2047"),

    ("spark.hadoop.hive.thrift.client.max.message.size", "2147483647"),
    ("spark.hadoop.hive.metastore.client.max.message.size", "2147483647"),
    ("spark.hadoop.hive.metastore.thrift.max.message.size", "2147483647"),
    ("spark.hadoop.hive.metastore.client.socket.timeout", "600"),

    (
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
        "com.crealytics:spark-excel_2.13:3.5.1_0.20.4,"
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.661"
    ),
    ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),

    ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

    ("spark.sql.catalog.csi_catalog", "org.apache.iceberg.spark.SparkCatalog"),
    ("spark.sql.catalog.csi_catalog.type", "hive"),
    ("spark.sql.catalog.csi_catalog.uri",
     "thrift://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30179"),

    ("spark.hadoop.wxd.cas.endpoint",
     "https://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30972/cas/v1/signature"),
    ("spark.hadoop.wxd.apikey", wxd_encoded_apikey),
    ("spark.hive.metastore.client.plain.username", wxd_hms_username),
    ("spark.hive.metastore.client.plain.password", wxd_hms_password),

    # COS S3A
    ("spark.hadoop.fs.s3a.bucket.sample-testing-data.access.key", cos_access_key),
    ("spark.hadoop.fs.s3a.bucket.sample-testing-data.secret.key", cos_secret_key),
    ("spark.hadoop.fs.s3a.bucket.sample-testing-data.endpoint", cos_endpoint),
    ("spark.hadoop.fs.s3a.bucket.sample-testing-data.path.style.access", "true"),
    ("spark.hadoop.fs.s3a.bucket.sample-testing-data.connection.ssl.enabled", "true"),
])

spark = (
    SparkSession.builder
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Spark version:", spark.version)
cos_key = "Sharepoint/FY26 Master Planning File/2026/01/05/20260105T131937_FY26 Master Planning File.xlsx"
bucket = os.getenv("BUCKET_NAME")
excel_path = f"s3a://{bucket}/{cos_key}"
try:
    spark_df = (
        spark.read
            .format("com.crealytics.spark.excel")
            .option("dataAddress", "'Vert San Plan'!A4:BE1048576")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("addColorColumns", "false")
            .load(excel_path)
    )

    # Clean column names
    spark_df = spark_df.select(
        [col(c).alias(c.strip()) for c in spark_df.columns]
    )

    print("Row count:", spark_df.count())

    # ------------------------------------------------------------------
    # Write to Iceberg
    # ------------------------------------------------------------------
    catalog_name = "csi_catalog"
    db_name = "sharepoint_csi_silver"
    table_name = "vert_san_plan"
    full_table_name = f"{catalog_name}.{db_name}.{table_name}"


    (
        spark_df
            .repartition(10)
            .write
            .format("iceberg")
            .mode("overwrite")
            .saveAsTable(full_table_name)
    )

    print("Iceberg write completed successfully.")

except Exception:
    print("Job failed. Traceback:")
    traceback.print_exc()

finally:
    spark.stop()
