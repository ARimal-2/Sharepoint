import os
import tempfile
from datetime import datetime, timezone
from boto3.s3.transfer import TransferConfig
from s3uploader.connect import connect_to_cos
from logs.ingestion_logger import save_ingestion_metadata
from logs.ingestion_logger import setup_logging_for_url

def upload_excel_to_cos(file_bytes: bytes, file_name: str):
    """
    Upload raw Excel bytes to IBM COS.
    """

    cos, bucket_name = connect_to_cos()
    outer_logs_dir, logs_dir, logger = setup_logging_for_url(file_name)
    utc_now = datetime.now(timezone.utc)
    date_path = utc_now.strftime("%Y/%m/%d")
    timestamp = utc_now.strftime("%Y%m%dT%H%M%S")
    fileName = file_name.replace('.xlsx','')
    object_key = (
        f"Sharepoint/{fileName}/"
        f"{date_path}/{file_name}"
    )

    transfer_config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024, 
        multipart_chunksize=50 * 1024 * 1024,  
        use_threads=True,
    )
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes)
            tmp.flush()
            tmp_path = tmp.name

        cos.upload_file(
            Filename=tmp_path,
            Bucket=bucket_name,
            Key=object_key,
            Config=transfer_config,
        )

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    save_ingestion_metadata(
        outer_logs_dir,
        fileName,
        timestamp,
        object_key
    )

    return object_key
