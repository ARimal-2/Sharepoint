import os
import ibm_boto3
from ibm_botocore.client import Config
from config import get_config

def connect_to_cos():
    api_key = get_config("API_KEY", required=True)
    service_instance_id = get_config("LH_INSTANCE_ID", required=True)
    endpoint_url = get_config("COS_ENDPOINT", required=True)
    bucket_name = get_config("BUCKET_NAME", required=True)
    if not all([api_key, service_instance_id, endpoint_url]):
        raise EnvironmentError("One or more IBM COS environment variables are missing.")

    cos = ibm_boto3.client(
        "s3",
        ibm_api_key_id=api_key,
        ibm_service_instance_id=service_instance_id,
        config=Config(signature_version="oauth"),
        endpoint_url=endpoint_url
    )
    return cos,bucket_name
