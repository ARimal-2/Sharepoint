import os
import base64
from io import BytesIO
import pandas as pd

from pyspark.sql import SparkSession
from pyspark import SparkConf
from s3uploader.connect import connect_to_cos

# -------------------------------
# 1. Environment Fix
# -------------------------------
os.environ["PYSPARK_ACCUMULATORS_ENABLED"] = "false"

# -------------------------------
# 2. Credentials & Authentication
# -------------------------------
wxd_username = "ankit.rimal@crushbank.com"
wxd_apikey = "1WGn3ZlQabGETxvCRwX8Ru13IUPUv2cRCQ0GT-G2aobVE"
wxd_hms_username = "ibmlhapikey_" + wxd_username
wxd_hms_password = wxd_apikey

cos_endpoint = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
cos_access_key = "6de1127078464ff6bb1c15935efd95c6"
cos_secret_key = "0cf9f4f337ebf747934c71d2f7e4c24e6cbefd4e64faa847"

# Encode Hive Metastore API key
wxd_encoded_apikey = "Basic " + base64.b64encode(
    f"{wxd_hms_username}:{wxd_hms_password}".encode("utf-8")
).decode("utf-8")

# -------------------------------
# 3. Spark Configuration
# -------------------------------
conf = SparkConf()

conf.setAll([
    ("spark.rpc.message.maxSize", "1024"),
    ("spark.hadoop.hive.thrift.client.max.message.size", "1073741824"),
    ("spark.hadoop.hive.metastore.client.socket.timeout", "600"),
    ("spark.hadoop.hive.metastore.client.connect.retry.delay", "5s"),

    # Packages
    ("spark.jars.packages",
     "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,com.crealytics:spark-excel_2.12:0.13.5"),

    # Iceberg
    ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    ("spark.sql.catalog.csi_catalog", "org.apache.iceberg.spark.SparkCatalog"),
    ("spark.sql.catalog.csi_catalog.type", "hive"),
    ("spark.sql.catalog.csi_catalog.uri",
     "thrift://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30179"),

    # Hive Metastore Auth
    ("spark.hadoop.wxd.cas.endpoint",
     "https://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30972/cas/v1/signature"),
    ("spark.hadoop.wxd.apikey", wxd_encoded_apikey),
    ("spark.hive.metastore.client.plain.username", wxd_hms_username),
    ("spark.hive.metastore.client.plain.password", wxd_hms_password),

    # COS Config for buckets
    ("spark.hadoop.fs.s3a.access.key", cos_access_key),
    ("spark.hadoop.fs.s3a.secret.key", cos_secret_key),
    ("spark.hadoop.fs.s3a.endpoint", cos_endpoint),
    ("spark.hadoop.fs.s3a.path.style.access", "true"),
    ("spark.hadoop.fs.s3a.connection.ssl.enabled", "true"),
])

# -------------------------------
# 4. Start Spark Session
# -------------------------------
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
print("Spark version:", spark.version)

# -------------------------------
# 5. Download Excel from COS
# -------------------------------
cos_key = "Sharepoint/FY26 Master Planning File/2026/01/05/20260105T131937_FY26 Master Planning File.xlsx"
cos, bucket = connect_to_cos()

response = cos.get_object(Bucket=bucket, Key=cos_key)
file_bytes = response['Body'].read()

# -------------------------------
# 6. Load into Pandas (optional)
# -------------------------------
excel_df = pd.read_excel(
    BytesIO(file_bytes),
    sheet_name="Vert San Plan",
    header=3,
    usecols="A:BE",
    engine='openpyxl'
)
print(f"Pandas DataFrame rows: {len(excel_df)}")
print("Head of DataFrame:\n", excel_df.head())

# -------------------------------
# 7. Convert to Spark DataFrame
# -------------------------------
spark_df = spark.createDataFrame(excel_df)
spark_df.printSchema()
catalog_name = "csi_catalog"
db_name = "sharepoint_csi_silver"
table_name = "vert_san_plan"
full_table_name = f"{catalog_name}.{db_name}.{table_name}"
spark_df.limit(0).write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)
# -------------------------------
# 8. Write to Iceberg
# -------------------------------





print(f"Writing data to {full_table_name}...")

spark_df.write.format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog_name}.{db_name}.{table_name}")


print("Successfully written to Iceberg.")

# -------------------------------
# 9. Stop Spark
# -------------------------------
spark.stop()
