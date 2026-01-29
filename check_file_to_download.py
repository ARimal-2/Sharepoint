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
    print(f"Reading metadata for resource: {resource_name} from COS bucket: {bucket_name}")
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


def parse_dt(dt_str: str) -> datetime:
    """Helper to parse various date formats."""
    # Remove Z and replace with +00:00 for fromisoformat
    clean_str = dt_str.replace("Z", "+00:00")
    
    try:
        # Try standard ISO format first (e.g. 2026-01-27T19:20:04 or 2026-01-27T19:20:04+00:00)
        return datetime.fromisoformat(clean_str)
    except ValueError:
        # Try basic format without separators (e.g. 20260127T192004)
        try:
            return datetime.strptime(clean_str, "%Y%m%dT%H%M%S")
        except ValueError:
            # Fallback for other possible formats if needed
            print(f"Warning: Could not parse date string '{dt_str}'. Attempting custom repair.")
            # Basic repair for YYYYMMDD to YYYY-MM-DD
            if len(dt_str) >= 8 and dt_str[4] != '-' and dt_str[7] != '-':
                 try:
                     # Attempt to parse YYYYMMDD...
                     return datetime.strptime(dt_str[:8], "%Y%m%d")
                 except:
                     pass
            raise

def should_download(sp_last_modified: str, last_extracted: str | None) -> bool:
    """
    Returns True if SharePoint file was modified after last extraction.
    """
    if last_extracted is None:
        return True  # First run: always download

    try:
        sp_dt = parse_dt(sp_last_modified).astimezone(timezone.utc)
        last_dt = parse_dt(last_extracted)
        
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        else:
            last_dt = last_dt.astimezone(timezone.utc)

        return last_dt < sp_dt
    except Exception as e:
        print(f"Error comparing dates: {e}. Defaulting to download.")
        return True

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
