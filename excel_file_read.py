import os
import requests
from urllib.parse import urlencode
from datetime import datetime

from metadata import (
    setup_logging_for_url,
    get_logger,
    save_extraction_metadata,
)
from check_file_to_download import (
    get_last_extracted_from_cos,
    should_download,
)
from s3uploader.upload_to_s3 import upload_excel_to_cos
from config import get_config


TENANT_ID = get_config("TENENT_ID", required=True)
CLIENT_ID = get_config("CLIENT_ID", required=True)
CLIENT_SECRET = get_config("CLIENT_SECRET", required=True)
USERNAME = get_config("USERNAME", required=True)
PASSWORD = get_config("PASSWORD", required=True)
HOSTNAME = get_config("HOSTNAME", required=True)
SITE_PATH = get_config("SITE_PATH", required=True)
file_ids = get_config("FILE_IDS", required=True)
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

# Authentication
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
        "scope": GRAPH_SCOPE,
    }

    res = requests.post(
        url,
        data=urlencode(payload),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    res.raise_for_status()
    return res.json()["access_token"]


# SharePoint 
def get_site_id(token):
    url = f"https://graph.microsoft.com/v1.0/sites/{HOSTNAME}:/sites/{SITE_PATH}"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()
    return res.json()["id"]


def get_drive_id(token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()
    return res.json()["value"][1]["id"]


def get_file_metadata(token, site_id, drive_id, file_id):
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        f"/drives/{drive_id}/items/{file_id}"
    )
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()
    return res.json()


def download_excel(token, site_id, drive_id, file_id):
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        f"/drives/{drive_id}/items/{file_id}/content"
    )
    res = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        allow_redirects=True,
    )
    res.raise_for_status()
    return res.content


# File Processor
def process_excel_file(
    token,
    site_id,
    drive_id,
    file_id,
    outer_logs_dir,
    timestamp,
    logger,
):
    sp_meta = get_file_metadata(token, site_id, drive_id, file_id)
    file_name = sp_meta["name"]
    last_modified = sp_meta["lastModifiedDateTime"]

    resource_name = file_name.replace(".xlsx", "").replace(" ", "_")

    last_ingested = get_last_extracted_from_cos(resource_name)

    print(
        f"Processing file: {file_name} | "
        f"SP last modified: {last_modified} | "
        f"COS last ingested: {last_ingested}"
    )

    if not should_download(last_modified, last_ingested):
        print(f"{file_name} is up to date. Skipping.")
        return False

    file_bytes = download_excel(token, site_id, drive_id, file_id)

    object_key = upload_excel_to_cos(
        file_bytes=file_bytes,
        file_name=file_name,
    )

    print(f"Uploaded {file_name} to COS: {object_key}")

    save_extraction_metadata(
        outer_logs_dir,
        file_name,
        timestamp,
    )

    print(f"Metadata written for {file_name}")
    return True


# Main
def excel_main():
    timestamp = datetime.utcnow()

    outer_logs_dir, _, log_file_path = setup_logging_for_url(
        "excel_file_read",
        timestamp,
    )
    logger = get_logger("excel_file_read", log_file_path, timestamp)

    try:
        print("Starting SharePoint Excel ingestion")

        token = get_access_token()
        site_id = get_site_id(token)
        drive_id = get_drive_id(token, site_id)

        for file_id in file_ids:
            process_excel_file(
                token=token,
                site_id=site_id,
                drive_id=drive_id,
                file_id=file_id,
                outer_logs_dir=outer_logs_dir,
                timestamp=timestamp,
                logger=logger,
            )

        print("All Excel files processed successfully")
        return True

    except Exception as e:
        logger.error("Pipeline failed", exc_info=True)
        raise


if __name__ == "__main__":
    excel_main()
