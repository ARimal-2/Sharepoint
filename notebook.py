# from pyspark.sql import functions as F
# import pandas as pd
# import base64
# import os
# from io import BytesIO
# from pyspark.sql import SparkSession
# from pyspark import SparkConf 
# from dotenv import load_dotenv
# import time,json
# import boto3
# from botocore.client import Config
# load_dotenv()

# wxd_username = os.getenv("wxd_username")
# wxd_apikey = os.getenv("wxd_apikey")
# wxd_hms_username = "ibmlhapikey_" + wxd_username

# wxd_hms_password = wxd_apikey


# # Encode HMS credentials
# string_to_encode = wxd_hms_username + ":" + wxd_hms_password
# wxd_encoded_apikey = "Basic " + base64.b64encode(string_to_encode.encode("utf-8")).decode("utf-8")

# wxd_hms_password = wxd_apikey

# cos_endpoint = os.getenv("IBM_COS_ENDPOINT") 
# cos_access_key=os.getenv("cos_access_key")
# cos_secret_key=os.getenv("cos_secret_key")
# print(wxd_encoded_apikey)
# print(wxd_hms_username)



# # Stop existing Spark session if any, gracefully handling if spark is not defined
# try:
#     # Only stop if a SparkSession object named 'spark' exists and is running
#     if 'spark' in locals() and spark is not None:
#         spark.stop()
# except Exception as e:
#     # Handle cases where spark might not be defined or other issues during stop
#     print(f"No existing Spark session to stop or an error occurred: {e}")

# # Initialize a new SparkConf object
# conf = SparkConf()

# conf.setAll([
#     # --- 1. General Spark & Performance ---
#     ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
#     ("spark.sql.debug.maxToStringFields", "1000"),
#     ("spark.sql.iceberg.vectorization.enabled", "false"),
    
#     # --- 2. Jars and Extensions ---
#     # Includes Iceberg runtime, Excel support, and AWS SDK
#     ("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,com.crealytics:spark-excel_2.13:3.5.1_0.20.4,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.661"),
#     ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
#     ("spark.sql.catalogImplementation", "hive"),

#     # --- 3. Hive Metastore (HMS) Security & Connectivity ---
#     ("spark.hive.metastore.uris", "thrift://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30179"),
#     ("spark.hive.metastore.use.SSL", "true"),
#     ("spark.hive.metastore.client.auth.mode", "PLAIN"),
#     ("spark.hive.metastore.client.plain.username", wxd_hms_username),
#     ("spark.hive.metastore.client.plain.password", wxd_hms_password),
#     ("spark.hive.metastore.truststore.path", "file:///opt/ibm/jdk/lib/security/cacerts"),
#     ("spark.hive.metastore.truststore.password", "changeit"),
#     ("spark.hive.metastore.truststore.type", "JKS"),
#     ("spark.hadoop.iceberg.engine.hive.lock-enabled", "false"),

#     # --- 4. Watsonx.data CAS & API Endpoints ---
#     ("spark.hadoop.wxd.cas.endpoint", "https://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:32456/cas/v1/signature"),
#     ("spark.hadoop.wxd.apikey", wxd_encoded_apikey),
#     ("spark.wxd.api.endpoint", "https://us-south.lakehouse.cloud.ibm.com"),

#     # --- 5. Iceberg Catalogs ---
#     # CSI Catalog
#     ("spark.sql.catalog.csi_catalog", "org.apache.iceberg.spark.SparkCatalog"),
#     ("spark.sql.catalog.csi_catalog.type", "hive"),
#     ("spark.sql.catalog.csi_catalog.warehouse", "s3a://csi-catalog-bucket/"),
    
#     # Iceberg Catalog
#     ("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),
#     ("spark.sql.catalog.iceberg_catalog.type", "hive"),
#     ("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://wastonx-data-bucket/"),

#     # --- 6. S3A Bucket Storage Configurations (using SimpleAWSCredentials) ---
    
#     # Bucket: csi-catalog-bucket
#     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
#     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.access.key", cos_access_key),
#     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.secret.key", cos_secret_key),
#     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.endpoint", cos_endpoint),
#     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.path.style.access", "true"),
#     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.connection.ssl.enabled", "true"),

#     # Bucket: bronze-csi-datalake
#     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
#     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.access.key", cos_access_key),
#     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.secret.key", cos_secret_key),
#     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.endpoint", cos_endpoint),
#     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.path.style.access", "true"),
#     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.connection.ssl.enabled", "true"),


#     # Bucket: sample-testing-data
#     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
#     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.access.key", cos_access_key),
#     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.secret.key", cos_secret_key),
#     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.endpoint", cos_endpoint),
#     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.path.style.access", "true"),
#     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.connection.ssl.enabled", "true")
# ])
# # conf.setAll([
# #     ("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1," "com.crealytics:spark-excel_2.13:3.5.1_0.20.4," "org.apache.hadoop:hadoop-aws:3.3.6," "com.amazonaws:aws-java-sdk-bundle:1.12.661" ),
# #     ("spark.hadoop.wxd.cas.endpoint","https://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30972/cas/v1/signature"),
# #     ("spark.hadoop.wxd.apikey", wxd_encoded_apikey),
   
# #    ( "spark.hive.metastore.use.SSL", "true"),
# #     ("spark.hive.metastore.client.auth.mode", "PLAIN"),
# #      ("spark.hive.metastore.client.plain.username", wxd_hms_username),
# #     ("spark.hive.metastore.client.plain.password", wxd_hms_password),
# #     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.aws.credentials.provider", 
# #     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
# #     ("spark.hive.metastore.truststore.password", "changeit"),
# #     ("spark.hive.metastore.truststore.type", "JKS"),
# #     ("spark.hive.metastore.truststore.path",
# #          "file:///usr/lib/jvm/java-17-openjdk-amd64/lib/security/cacerts"),
# #         # ============= Csi-catalog-bucket COS =============
# #     ("spark.sql.catalog.csi_catalog.type", "hive"),
# #     ("spark.sql.catalog.csi_catalog.uri", "thrift://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30179"),
# #     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.access.key", cos_access_key),
# #     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.secret.key", cos_secret_key),
# #     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.endpoint", cos_endpoint),
# #     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.path.style.access", "true"),
# #     ("spark.hadoop.fs.s3a.bucket.csi-catalog-bucket.connection.ssl.enabled", "true"),
    
# #             # =============  bronze-csi-datalake COS =============
# #     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.access.key", cos_access_key),
# #     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.secret.key", cos_secret_key),
# #     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.endpoint", cos_endpoint),
# #     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.path.style.access", "true"),
# #     ("spark.hadoop.fs.s3a.bucket.bronze-csi-datalake.connection.ssl.enabled", "true"),
# #     ("spark.sql.debug.maxToStringFields", "1000"),
# #     ("spark.sql.catalog.csi_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
# #       ("spark.sql.catalog.csi_catalog.type", "hive"),
# #                 # =============  sample-testing-data COS =============
# #     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.access.key", cos_access_key),
# #     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.secret.key", cos_secret_key),
# #     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.endpoint", cos_endpoint),
# #     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.path.style.access", "true"),
# #     ("spark.hadoop.fs.s3a.bucket.sample-testing-data.connection.ssl.enabled", "true"), 

# # ])


# # Start Spark session with updated config
# spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# # Verify configuration
# spark_conf = spark.sparkContext.getConf().getAll()
# for key, value in spark_conf:
#     print(f"{key}: {value}")

#     print("Listing catalogs...")
#     spark.sql("SHOW CATALOGS").show()
# cos_client = boto3.client(
#         "s3",
#         aws_access_key_id=cos_access_key,
#         aws_secret_access_key=cos_secret_key,
#         endpoint_url=cos_endpoint,
#         config=Config(signature_version="s3v4")
#     )

# bucket_name = 'sample-testing-data'
# catalog_name = "csi_catalog"  
# db_name = "sharepoint_csi_silver" 

# cos_key = "Sharepoint/FY26 Master Planning File/2026/01/05/20260105T131937_FY26 Master Planning File.xlsx"
# response = cos_client.get_object(Bucket=bucket_name, Key=cos_key)
# file_bytes = response['Body'].read()

# excel_df = pd.read_excel(
#     BytesIO(file_bytes),
#     sheet_name="Vert San Plan",
#     header=3,
#     usecols="A:BE",
#     engine='openpyxl'
# )
# print("Excel DataFrame loaded. Preview:")
# print(excel_df.head())
# spark_df = spark.createDataFrame(excel_df)
# full_table_name = f"{catalog_name}.{db_name}.vert_san_plan"
# spark_df.head()
# spark_df.printSchema()
# table_name = "vert_san_plan1"
# print(f"Writing data to {table_name}...")
# spark_df.write.format("iceberg") \
#     .mode("append") \
#     .saveAsTable(full_table_name)    
