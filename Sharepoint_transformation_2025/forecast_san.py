from pyspark.sql import SparkSession
from config import get_config

def vert_fore_san_25_transform_and_load(spark: SparkSession):
    bucket_name = get_config("BUCKET_NAME", required=True)
    cos_key = get_config("cos_key_2025", required=True)
    excel_path = f"s3a://{bucket_name}/{cos_key}"

    catalog_name = get_config("catalog_name", required=True)
    db_name = get_config("db_name", required=True)
    table_name = "Forecast_SAN_FY2025"
    full_table_name = f"{catalog_name}.{db_name}.{table_name}"

    sheet_name = "Forecasts"
    data_range = "DJ1:EI1000"
    week_column = None
    city = "San Antonio"
    state = "Texas"
    pdwks_range = "A2193:B2556"

    # No reading here; main process handles it
    return None, full_table_name, excel_path, sheet_name, data_range, week_column, city, state, pdwks_range
