import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")
service_instance_id = os.getenv("SERVICE_INSTANCE_ID")
endpoint_url = os.getenv("IBM_COS_ENDPOINT")
bucket_name = "sample-testing-data"
prefix = "Sharepoint/"  # note the trailing slash

cos = ibm_boto3.client(
    "s3",
    ibm_api_key_id=api_key,
    ibm_service_instance_id=service_instance_id,
    config=Config(signature_version="oauth"),
    endpoint_url=endpoint_url
)

# List all objects under the prefix
try:
    response = cos.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        print(f"Deleting {len(objects_to_delete)} objects under '{prefix}'...")
        delete_response = cos.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        print("Deleted successfully:", delete_response)
    else:
        print(f"No objects found under prefix '{prefix}'")
except ClientError as e:
    print(f"Error: {e}")
