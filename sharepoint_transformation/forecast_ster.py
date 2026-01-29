from pyspark.sql import SparkSession
from config import get_config

def vert_fore_ster_transform_and_load(spark: SparkSession):
    bucket_name = get_config("BUCKET_NAME", required=True)
    cos_key = get_config("cos_key", required=True)
    excel_path = f"s3a://{bucket_name}/{cos_key}"

    catalog_name = get_config("catalog_name", required=True)
    db_name = get_config("db_name", required=True)
    table_name = "Forecast_STER_FY2026"
    full_table_name = f"{catalog_name}.{db_name}.{table_name}"

    sheet_name = "Forecasts"
    data_range = "BK1:DN1000"
    week_column = None
    city = "Sterling"
    state = "Virginia"
    pdwks_range = "A2557:B2920"

    # No reading here; main process handles it
    return None, full_table_name, excel_path, sheet_name, data_range, week_column, city, state, pdwks_range
