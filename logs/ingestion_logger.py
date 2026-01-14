import os
import logging
import json
from datetime import datetime,timezone
from s3uploader.connect import connect_to_cos

# def setup_logging_for_url(resource_name):
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     outer_logs_dir = os.path.join("logs", "ingestion", resource_name)
#     os.makedirs(outer_logs_dir, exist_ok=True)

#     logs_dir = os.path.join(outer_logs_dir, timestamp)
#     os.makedirs(logs_dir, exist_ok=True)
#     log_file_path = os.path.join(logs_dir, 'ingestion.log')
        
#     # Reset logging to avoid duplicate handlers
#     for handler in logging.root.handlers[:]:
#         logging.root.removeHandler(handler)

#     logging.basicConfig(
#         filename=log_file_path,
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s'
#     )

#     console = logging.StreamHandler()
#     console.setLevel(logging.INFO)
#     formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     console.setFormatter(formatter)
#     logging.getLogger('').addHandler(console)

#     logging.info(f"Logging initialized in {logs_dir}")
#     return outer_logs_dir, logs_dir

# logs/ingestionlogger.py
def setup_logging_for_url(resource_name):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = resource_name.replace('.xlsx','')

    outer_logs_dir = os.path.join("logs", "ingestion", resource_name)
    os.makedirs(outer_logs_dir, exist_ok=True)
    logs_dir = os.path.join(outer_logs_dir, timestamp)
    os.makedirs(logs_dir, exist_ok=True)
    log_file_path = os.path.join(logs_dir, f'{resource_name}.log')

    # Dedicated logger
    logger = logging.getLogger(f"{resource_name}_{timestamp}")
    logger.setLevel(logging.INFO)

    # Remove old handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler
    fh = logging.FileHandler(log_file_path, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    # Console handler (safe)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

    logger.info(f"Logging initialized in {logs_dir}")
    return outer_logs_dir, logs_dir, logger

def save_ingestion_metadata(outer_logs_dir, resource_name, ingestion_date,object_key):
    """
    Saves metadata about the ingestion to a JSON file and uploads it to IBM COS.
    """
    metadata_path = os.path.join(outer_logs_dir, f"{resource_name}_metadata.json")
    metadata = {
        "ingestion_date": ingestion_date,
        "file_path":object_key,
        "history": [],
        "last_ingested": ingestion_date
    }

    # Preserve existing history
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                existing_metadata = json.load(f)
                history = existing_metadata.get("history", [])
                history.append({
                    "ingestion_date": existing_metadata.get("ingestion_date"),
                    "file_path":existing_metadata.get("file_path"),
                })
                metadata["history"] = history
    except Exception as e:
        logging.warning(f"Failed to read existing metadata to preserve history: {e}")

    # Save locally
    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        logging.info(f"Metadata saved locally at {metadata_path}")
    except Exception as e:
        logging.error(f"Error while saving metadata: {e}")
        return
    
    cos, bucket_name = connect_to_cos()
    metadatakey = f"Sharepoint/{resource_name}/{resource_name}_metadata.json"
    # print(metadatakey)

    # Upload to IBM COS
    try:
        json_bytes = json.dumps(metadata, indent=2).encode("utf-8")
        response = cos.put_object(Bucket=bucket_name, Key=metadatakey, Body=json_bytes)
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        logging.info(f"Metadata uploaded to COS at key: {metadatakey} with status {status_code}")
    except Exception as e:
        logging.error(f"Error uploading metadata to COS: {e}")