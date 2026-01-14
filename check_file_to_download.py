import json
from pathlib import Path
from datetime import datetime, timezone


# -----------------------------
# Extraction metadata reader
# -----------------------------
def get_last_extracted_from_extraction_metadata(metadata_path: str):
    """
    Reads last_extracted timestamp from extraction metadata.
    """
    if not Path(metadata_path).exists():
        return None

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    return metadata.get("last_extracted")


# -----------------------------
# Download decision logic
# -----------------------------
def should_download(sp_last_modified: str, last_extracted: str | None) -> bool:
    """
    Returns True if SharePoint file was modified after last extraction.
    """

    # First run — always download
    if last_extracted is None:
        return True

    # SharePoint timestamp (always UTC)
    sp_dt = datetime.fromisoformat(
        sp_last_modified.replace("Z", "+00:00")
    ).astimezone(timezone.utc)

    # Extraction timestamp (force UTC)
    local_dt = datetime.fromisoformat(last_extracted)
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=timezone.utc)
    else:
        local_dt = local_dt.astimezone(timezone.utc)

    return local_dt < sp_dt
