import os
import zipfile
import requests
import boto3
import base64
from config import get_config
from connect_to_cos import connect_to_cos

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
        for f in ["config.py", "transformation.py","connect_to_cos.py"]:
            z.write(os.path.join(base_dir, f), arcname=f)

        # # minimal netsuite_api_transform package
        # z.write(
        #     os.path.join(base_dir, "netsuite_api_transform", "__init__.py"),
        #     arcname="netsuite_api_transform/__init__.py",
        # )

        # z.write(
        #     os.path.join(base_dir, "netsuite_api_transform", "department.py"),
        #     arcname="netsuite_api_transform/department.py",
        # )

    return zip_name



def upload_to_cos(file_path):
    s3 = connect_to_cos()
    
    object_name = os.path.basename(file_path)
    s3.upload_file(file_path, BUCKET_NAME, object_name)
    print(f"Uploaded {file_path} to bucket {BUCKET_NAME} as {object_name}")

if __name__ == "__main__":
    zip_file = prepare_package()
    upload_to_cos("main.py")
    
    upload_to_cos(zip_file)