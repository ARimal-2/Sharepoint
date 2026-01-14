import os
import logging
import json
import hashlib
from datetime import datetime


# -----------------------------
# Logging and directories setup
# -----------------------------
def setup_logging_for_url(resource_name: str, timestamp: datetime):
    """
    Returns the outer_logs_dir, logs_dir, log_file_path
    No id_file_path is created.
    """
    current_time = timestamp.strftime("%Y%m%d_%H%M%S")
    
    outer_logs_dir = os.path.join("logs", "extraction", resource_name)
    os.makedirs(outer_logs_dir, exist_ok=True)

    logs_dir = os.path.join(outer_logs_dir, current_time)
    os.makedirs(logs_dir, exist_ok=True)

    log_file_path = os.path.join(logs_dir, f"{resource_name}.log")
    return outer_logs_dir, logs_dir, log_file_path


def get_logger(resource_name: str, log_file_path: str, timestamp: datetime = None):
    """
    Returns a logger object writing to a single .log file.
    """
    timestamp_str = (
        timestamp.strftime("%Y%m%d_%H%M%S")
        if timestamp else datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    )

    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger_name = f"{resource_name}_{timestamp_str}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    fh = logging.FileHandler(log_file_path)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    logger.info(f"Logger initialized for {resource_name}")
    return logger


# -----------------------------
# Extraction metadata handling
# -----------------------------
# def save_extraction_metadata(outer_logs_dir, resource_name, extraction_date):
#     os.makedirs(outer_logs_dir, exist_ok=True)

#     metadata_path = os.path.join(
#         outer_logs_dir, f"{resource_name}_metadata.json"
#     )

#     extraction_date_str = extraction_date.isoformat()

#     # Load existing metadata if present
#     if os.path.exists(metadata_path):
#         with open(metadata_path, "r", encoding="utf-8") as f:
#             metadata = json.load(f)

#         history = metadata.get("history", [])
#     else:
#         metadata = {}
#         history = []

#     # Append current extraction to history
#     history.append({
#         "extraction_date": extraction_date_str
#     })

#     # Update metadata
#     metadata["resource_name"] = resource_name
#     metadata["last_extracted"] = extraction_date_str
#     metadata["history"] = history

#     # Persist metadata
#     with open(metadata_path, "w", encoding="utf-8") as f:
#         json.dump(metadata, f, indent=4)

#     return metadata_path

import os
import json
import logging
from datetime import datetime


def save_extraction_metadata(
    outer_logs_dir: str,
    resource_name: str,
    extraction_date: datetime
):
    """
    Persist extraction metadata with history.
    """

    os.makedirs(outer_logs_dir, exist_ok=True)
    metadata_path = os.path.join(
        outer_logs_dir, f"{resource_name}_metadata.json"
    )

    extraction_date_str = extraction_date.isoformat()
    history = []

    # Load existing metadata if it exists
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                existing_metadata = json.load(f)

            # Preserve existing history
            history = existing_metadata.get("history", [])

            # Move previous last_extracted into history
            previous = existing_metadata.get("last_extracted")
            if previous:
                history.append({"extracted_at": previous})

        except Exception as e:
            logging.warning(f"Failed to read existing metadata: {e}")
            history = []

    # Write updated metadata
    metadata = {
        "resource_name": resource_name,
        "last_extracted": extraction_date_str,
        "history": history
    }

    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)

        logging.info(f"Metadata saved to {metadata_path}")

    except Exception as e:
        logging.error(f"Failed to write metadata: {e}")
        raise

# -----------------------------
# Last extraction loader
# -----------------------------
def load_last_extraction_time(outer_logs_dir, resource_name):
    """
    Loads the last extraction timestamp from metadata if available.
    Returns last_extracted or None if first load.
    """
    metadata_file = os.path.join(outer_logs_dir, f"{resource_name}_metadata.json")

    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            return metadata.get("last_extracted")
        except Exception as e:
            logging.warning(f"Could not read metadata file: {e}")
            return None

    logging.info("Metadata file not found — treating as first load.")
    return None


# -----------------------------
# SharePoint metadata handling
# -----------------------------
def sha256_checksum(file_path: str) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_metadata(file_path: str, sp_file: dict, duration_ms: int, logger=None):
    """
    Writes SharePoint file metadata alongside the downloaded file.
    """
    try:
        checksum = sha256_checksum(file_path)

        metadata = {
            "source_system": "sharepoint",
            "file_name": sp_file["name"],
            "file_id": sp_file.get("id"),
            "last_modified_datetime": sp_file["lastModifiedDateTime"],
            "checksum_sha256": checksum,
            "download_duration_ms": duration_ms,
            "generated_at_utc": datetime.utcnow().isoformat()
        }

        metadata_path = f"{file_path}.metadata.json"

        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        if logger:
            logger.info(f"Metadata written: {metadata_path}")

        return metadata_path

    except Exception as e:
        if logger:
            logger.error(f"Failed to write metadata for {file_path}: {e}")
        raise

