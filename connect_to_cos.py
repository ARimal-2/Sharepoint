import os
import zipfile
import requests
import boto3
import base64
from config import get_config

BUCKET_NAME = get_config("BUCKET_NAME", required=True)
COS_ENDPOINT = get_config("COS_ENDPOINT", required=True)
COS_ACCESS_KEY = get_config("COS_ACCESS_KEY", required=True)
COS_SECRET_KEY = get_config("COS_SECRET_KEY", required=True)
INSTANCE_ROUTE = get_config("INSTANCE_ROUTE", required=True)

def connect_to_cos():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=COS_ACCESS_KEY,
        aws_secret_access_key=COS_SECRET_KEY,
        endpoint_url=COS_ENDPOINT,
    )
    return s3