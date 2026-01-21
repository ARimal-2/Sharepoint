from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from sharepoint_transformation.vert_san_plan import vert_san_plan_transform_and_load
def transform_and_load(spark: SparkSession):
    df,full_table_name,excel_path = vert_san_plan_transform_and_load(spark)
    # bucket_name = "sample-testing-data"
    # cos_key = "Sharepoint/FY26 Master Planning File/2026/01/14/FY26 Master Planning File.xlsx"
    # excel_path = f"s3a://{bucket_name}/{cos_key}"

    # catalog_name = "csi_catalog"
    # db_name = "sharepoint_csi_silver"
    # table_name = "vert_san_plan"
    # full_table_name = f"{catalog_name}.{db_name}.{table_name}"


    df = (
        spark.read
            .format("com.crealytics.spark.excel")
            .option("dataAddress", "'Vert San Plan'!A4:BE1000") 
            .option("header", "true")                           
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", "false")                      
            .option("addColorColumns", "false")
            .option("maxRowsInMemory", 10000)
            .load(excel_path)
    )

    print("Rows read:", df.count())

    # -----------------------------
    # Clean column names (replace spaces, dots, slashes, etc.)
    # -----------------------------
    def clean(col_name: str) -> str:
        return (
            col_name.strip()
                    .replace(" ", "_")
                    .replace(".", "_")
                    .replace("-", "_")
                    .replace("/", "_")
        )

    df = df.select([F.col(c).alias(clean(c)) for c in df.columns])


    if not spark.catalog.tableExists(full_table_name):
        print(f"Creating table {full_table_name}...")
        df.repartition(10).writeTo(full_table_name).create()
    else:
        print(f"Overwriting table {full_table_name}...")
        df.repartition(10).write \
            .format("iceberg") \
            .mode("overwrite") \
            .save(full_table_name)

    print(f"Data successfully written to {full_table_name}")
