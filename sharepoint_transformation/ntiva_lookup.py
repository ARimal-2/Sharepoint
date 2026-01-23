from pyspark.sql import SparkSession
from config import get_config

def ntiva_lookup_load(spark: SparkSession):
    bucket_name = get_config("BUCKET_NAME", required=True)
    cos_key = get_config("lookup_cos_key", required=True)
    excel_path = f"s3a://{bucket_name}/{cos_key}"

    catalog_name = get_config("catalog_name", required=True)
    db_name = get_config("db_name", required=True)
    table_name = "ntiva_lookup"
    full_table_name = f"{catalog_name}.{db_name}.{table_name}"

    sheet_name = "Lookup Table"
    data_range = "B2:F1000"

    # No reading here; main process handles it
    return None, full_table_name, excel_path, sheet_name, data_range, None, None, None
