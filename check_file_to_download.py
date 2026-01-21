import json
from pathlib import Path
from datetime import datetime, timezone
from s3uploader.connect import connect_to_cos


def get_last_extracted_from_cos(resource_name: str):
    """
    Reads last_extracted timestamp from COS metadata JSON.
    Returns None if metadata does not exist.
    """
    cos, bucket_name = connect_to_cos()
    metadata_key = f"Sharepoint/{resource_name}/{resource_name}_metadata.json"

    try:
        response = cos.get_object(Bucket=bucket_name, Key=metadata_key)
        metadata_bytes = response['Body'].read()
        metadata = json.loads(metadata_bytes)
        print(f"Metadata from COS: {metadata}")
        return metadata.get("last_ingested") 
    except cos.exceptions.NoSuchKey:
        # First run: metadata not uploaded yet
        return None
    except Exception as e:
        print(f"Error reading metadata from COS: {e}")
        return None

# # -----------------------------
# # Extraction metadata reader
# # -----------------------------
# def get_last_extracted_from_extraction_metadata(metadata_path: str):
#     """
#     Reads last_extracted timestamp from extraction metadata.
#     """
#     if not Path(metadata_path).exists():
#         return None

#     with open(metadata_path, "r", encoding="utf-8") as f:
#         metadata = json.load(f)

#     return metadata.get("last_extracted")


def should_download(sp_last_modified: str, last_extracted: str | None) -> bool:
    """
    Returns True if SharePoint file was modified after last extraction.
    """
    if last_extracted is None:
        return True  # First run: always download

    sp_dt = datetime.fromisoformat(sp_last_modified.replace("Z", "+00:00")).astimezone(timezone.utc)
    last_dt = datetime.fromisoformat(last_extracted)
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    else:
        last_dt = last_dt.astimezone(timezone.utc)

    return last_dt < sp_dt

# # -----------------------------
# # Download decision logic
# # -----------------------------
# def should_download(sp_last_modified: str, last_extracted: str | None) -> bool:
#     """
#     Returns True if SharePoint file was modified after last extraction.
#     """

#     # First run — always download
#     if last_extracted is None:
#         return True

#     # SharePoint timestamp (always UTC)
#     sp_dt = datetime.fromisoformat(
#         sp_last_modified.replace("Z", "+00:00")
#     ).astimezone(timezone.utc)

#     # Extraction timestamp (force UTC)
#     local_dt = datetime.fromisoformat(last_extracted)
#     if local_dt.tzinfo is None:
#         local_dt = local_dt.replace(tzinfo=timezone.utc)
#     else:
#         local_dt = local_dt.astimezone(timezone.utc)

#     return local_dt < sp_dt
