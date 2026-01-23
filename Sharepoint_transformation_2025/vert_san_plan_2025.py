from pyspark.sql import SparkSession
from config import get_config

def vert_san_25_plan_transform_and_load(spark: SparkSession):
    bucket_name = get_config("BUCKET_NAME", required=True)
    cos_key = get_config("cos_key_2025", required=True)
    excel_path = f"s3a://{bucket_name}/{cos_key}"

    catalog_name = get_config("catalog_name", required=True)
    db_name = get_config("db_name", required=True)
    table_name = "Vert_SAN_Plan_FY2025"
    full_table_name = f"{catalog_name}.{db_name}.{table_name}"

    sheet_name = "Vert_SAN_Plan"
    data_range = "D2:ND1000"
    week_column = "DofW"
    city = "San Antonio"
    state = "Texas"

    # No reading here; main process handles it
    return None, full_table_name, excel_path, sheet_name, data_range, week_column, city, state  
