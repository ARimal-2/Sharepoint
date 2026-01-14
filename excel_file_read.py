import os
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote
from dotenv import load_dotenv
from urllib.parse import urlencode
from metadata import write_metadata, setup_logging_for_url, get_logger,save_extraction_metadata
from check_file_to_download import should_download,get_last_extracted_from_extraction_metadata,get_last_extracted_from_extraction_metadata
from datetime import datetime
from s3uploader.upload_to_s3 import upload_excel_to_cos
import time
load_dotenv()


TENANT_ID = os.getenv("TENENT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
HOSTNAME = os.getenv("HOSTNAME")
SITE_PATH = os.getenv("SITE_PATH")
FOLDER_PATH = os.getenv("FOLDER_PATH")
EXCEL_EXTENSION = ".xlsx"

GRAPH_SCOPE = "https://graph.microsoft.com/.default"


# ---------------- TOKEN ----------------

def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username":"ntiva_api_service@cuisinesolutions.com",
        "password":PASSWORD,
        "scope": "https://graph.microsoft.com/.default",
    }
    
    print(f"payload:{payload}")
    encoded_query_string = urlencode(payload)
    res = requests.post(url, data=encoded_query_string,headers={"Content-Type":"application/x-www-form-urlencoded"})
    print(res)
    try:
        res.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"Token acquisition failed: {e}\nBody: {res.text}")
    return res.json()["access_token"]

# ---------------- SITE ----------------
def get_site_id(token):
    url = (
        f"https://graph.microsoft.com/v1.0/sites/"
        f"{HOSTNAME}:/sites/{SITE_PATH}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()["id"]


# # ---------------- DRIVE ----------------
def get_drive_id(token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()

    drives = res.json()["value"]


    return drives[1]["id"]


# ---------------- FILE ----------------
# def get_file_id(token,site_id, drive_id, folder_path):
#     print(f"{token}{site_id}{drive_id}")
#     encoded_path = quote(folder_path)
#     print("encoded:",encoded_path)

#     url = (
#         f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/"
#         f"{drive_id}/root:/{folder_path}:/children"
#     )
#     headers = {"Authorization": f"Bearer {token}"}
#     print(url)
#     res = requests.get(url, headers=headers)
#     res.raise_for_status()
#     print("item_response",res.json())

#     for item in res.json()["value"]:
#         if item["name"].lower().endswith(EXCEL_EXTENSION.lower()):
#             return item["id"], item["name"]

#     raise FileNotFoundError("No Excel file found")

# def get_file(token, site_id, drive_id, folder_path):
#     encoded_path = quote(folder_path)
#     url = (
#         f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/"
#         f"{drive_id}/root:/{encoded_path}:/children"
#     )
#     headers = {"Authorization": f"Bearer {token}"}
#     res = requests.get(url, headers=headers)
#     res.raise_for_status()

#     for item in res.json()["value"]:
#         if item["name"].lower().endswith(EXCEL_EXTENSION.lower()):
#             return item

#     raise FileNotFoundError("No Excel file found")

# # ---------------- DOWNLOAD ----------------
def download_excel(token, site_id, drive_id, item_id):
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/"
        f"{drive_id}/items/{item_id}/content"
    )

    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers, allow_redirects=True)
    res.raise_for_status()

    return res.content
def get_file_metadata(token, site_id, drive_id, item_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()


# ---------------- MAIN ----------------
def main():
    timestamp = datetime.utcnow()
    outer_logs_dir, logs_dir, log_file_path = setup_logging_for_url("excel_file_read", timestamp)
    logger = get_logger("excel_file_read", log_file_path, timestamp)
    

    try:
        logger.info("Starting Excel file download process")
        token = get_access_token()
        logger.info("Access token obtained successfully")
        site_id = get_site_id(token) 
        # print("site_id:",site_id)
        
        drive_id = get_drive_id(token,site_id)
        import requests




      
        
        print(f" Drive ID: {drive_id}")

    #     file_id, file_name = get_file_id(
    #     token=token,
    #     site_id=site_id,
    #     drive_id=drive_id,
    #     folder_path=FOLDER_PATH
    # )
        # print("file_id:",file_id)
        # print("file_name:",file_name)
        file_id = "01X4CHHEWSEED25RZHINDJK7ZRNZOQHJD5"   
        # Fetch metadata
        sp_file_meta = get_file_metadata(token, site_id, drive_id, file_id)
        file_name = sp_file_meta["name"]
        sp_last_modified = sp_file_meta["lastModifiedDateTime"]


        

        local_last_modified = get_last_extracted_from_extraction_metadata(
            os.path.join(outer_logs_dir, "excel_file_read_metadata.json")
        )

        logger.info(
            f"Local last extracted: {local_last_modified}, "
            f"SP last modified: {sp_last_modified}"
        )

        if not should_download(sp_last_modified, local_last_modified):
            logger.info("Local file is up to date. Skipping download.")
            return

        logger.info("SharePoint file is updated. Downloading...")

        # Download file content
        sp_file_content = download_excel(token, site_id, drive_id, file_id)
        object_key = upload_excel_to_cos(
            file_bytes=sp_file_content,
            file_name=file_name
        )
        print(f"Uploaded to COS: {object_key}")
        # local_file_path = f"downloads/{file_name}"
        # os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        # # Ensure directory exists
        # os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        # # Write Excel file
        # with open(local_file_path, "wb") as f:
        #      f.write(sp_file_content)

        # logger.info(f"File downloaded to: {local_file_path}")

        # Save metadata
        metadata_path = save_extraction_metadata(
            outer_logs_dir, "excel_file_read", timestamp
        )

        logger.info(f"Metadata written to: {metadata_path}")

        # # Load Excel
        # df = pd.read_excel(local_file_path)
        # logger.info(f"Excel file loaded. Rows: {len(df)}")

        logger.info("Download completed successfully.")

        
    except Exception as e:
        logger.error(f"Error during execution: {e}", exc_info=True)
        raise
if __name__ == "__main__":
    main()
