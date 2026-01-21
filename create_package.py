import os
import zipfile
import requests
import boto3
import base64
from config import get_config
from s3uploader.connect import connect_to_cos

BUCKET_NAME = get_config("BUCKET_NAME", required=True)
COS_ENDPOINT = get_config("COS_ENDPOINT", required=True)
COS_ACCESS_KEY = get_config("COS_ACCESS_KEY", required=True)
COS_SECRET_KEY = get_config("COS_SECRET_KEY", required=True)
INSTANCE_ROUTE = get_config("INSTANCE_ROUTE", required=True)

def prepare_package():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    zip_name = os.path.join(base_dir, "dependencies.zip")

    if os.path.exists(zip_name):
        os.remove(zip_name)

    with zipfile.ZipFile(zip_name, "w") as z:
        # top-level modules
        for f in ["config.py", "alternative_transformation.py","excel_file_read.py","check_file_to_download.py","metadata.py"]:
            z.write(os.path.join(base_dir, f), arcname=f)

        # # minimal netsuite_api_transform package
        # z.write(
        #     os.path.join(base_dir, "logs", "__init__.py"),
        #     arcname="logs/__init__.py",
        # )
        z.write(
            os.path.join(base_dir, "sharepoint_transformation", "__init__.py"),
            arcname="sharepoint_transformation/__init__.py",
        )
        z.write(
            os.path.join(base_dir, "sharepoint_transformation", "vert_san_plan.py"),
            arcname="sharepoint_transformation/vert_san_plan.py",
        )
        z.write(
            os.path.join(base_dir, "sharepoint_transformation", "ntiva_lookup.py"),
            arcname="sharepoint_transformation/ntiva_lookup.py",
        )
        z.write(
            os.path.join(base_dir, "sharepoint_transformation", "vert_alex_plan.py"),
            arcname="sharepoint_transformation/vert_alex_plan.py",
        )

        z.write(
            os.path.join(base_dir, "sharepoint_transformation", "vert_ster_plan.py"),
            arcname="sharepoint_transformation/vert_ster_plan.py",
        )
        z.write(
            os.path.join(base_dir, "logs", "__init__.py"),
            arcname="logs/__init__.py",
        )
        z.write(
            os.path.join(base_dir, "logs", "ingestion_logger.py"),
            arcname="logs/ingestion_logger.py",
        )
        # s3uploader package
        z.write(
            os.path.join(base_dir, "s3uploader", "__init__.py"),
            arcname="s3uploader/__init__.py",
        )

        z.write(
            os.path.join(base_dir, "s3uploader", "connect.py"),
            arcname="s3uploader/connect.py",
        )

        z.write(
            os.path.join(base_dir, "s3uploader", "upload_to_s3.py"),
            arcname="s3uploader/upload_to_s3.py",
        )


    return zip_name



def upload_to_cos(file_path):
    FOLDER = "Sharepoint_dep"
    s3,bucket = connect_to_cos()
    # object_name = os.path.join(FOLDER, os.path.basename(file_path))
    object_name = f"{FOLDER}/{os.path.basename(file_path)}".replace("\\", "/")
    
    s3.upload_file(file_path, bucket, object_name)
    print(f"Uploaded {file_path} to bucket {bucket} as {object_name}")

def create_package():
    zip_file = prepare_package()
    upload_to_cos("main.py")
    
    upload_to_cos(zip_file)

if __name__ == "__main__":
    create_package()