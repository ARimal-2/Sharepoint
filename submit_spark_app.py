import base64
import time
import requests
from config import get_config

BUCKET_NAME = get_config("BUCKET_NAME", required=True)
COS_ENDPOINT = get_config("COS_ENDPOINT", required=True)
COS_ACCESS_KEY = get_config("COS_ACCESS_KEY", required=True)
COS_SECRET_KEY = get_config("COS_SECRET_KEY", required=True)
INSTANCE_ROUTE = get_config("INSTANCE_ROUTE", required=True)
LH_INSTANCE_ID = get_config("LH_INSTANCE_ID", required=True)
SPARK_ENGINE_ID = get_config("SPARK_ENGINE_ID", required=True)
API_KEY = get_config("API_KEY", required=True)
WXD_USERNAME = get_config("WXD_USERNAME", required=True)
WXD_APIKEY = get_config("WXD_APIKEY", required=True)
BRONZE_BUCKET_NAME = get_config("bronze_bucket_name",required=True)
wxd_hms_username = "ibmlhapikey_" + WXD_USERNAME
wxd_hms_password = WXD_APIKEY
string_to_encode = wxd_hms_username + ":" + wxd_hms_password
wxd_encoded_apikey = "Basic " + base64.b64encode(string_to_encode.encode("utf-8")).decode("utf-8")


def get_iam_token():
    """Get IBM Cloud IAM token"""
    url = "https://iam.cloud.ibm.com/identity/token"
    data = (
        "grant_type=urn:ibm:params:oauth:grant-type:apikey"
        f"&apikey={API_KEY}"
    )
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=data, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

def submit_job(token):
    """Submit Spark job to IBM watsonx.data"""
    url = f"{INSTANCE_ROUTE}/lakehouse/api/v3/spark_engines/{SPARK_ENGINE_ID}/applications"
    
    payload = {
        "application_details": {
            "application": f"s3a://{BUCKET_NAME}/Sharepoint_dep/main.py",
            "spark_version": "3.4",
            "conf": {
                # Basic configs
                "spark.app.name": "sharepoint",
                "spark.submit.pyFiles": f"s3a://{BUCKET_NAME}/Sharepoint_dep/dependencies.zip",
                 "spark.jars.packages": "com.crealytics:spark-excel_2.12:0.13.7",
                "spark.driver.memory": "4G",
                "spark.executor.memory": "4G",
                "spark.sql.legacy.timeParserPolicy": "LEGACY",
                "ae.spark.executor.count": "1",
                "spark.hadoop.fs.s3a.path.style.access": "true",
"spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
"spark.hadoop.iceberg.engine.hive.lock-enabled": "true",


                "spark.hive.metastore.uris": "thrift://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30179",
                "spark.hive.metastore.use.SSL": "true",
                "spark.hive.metastore.client.auth.mode": "PLAIN",
                "spark.hive.metastore.client.plain.username": wxd_hms_username,
                "spark.hive.metastore.client.plain.password": wxd_hms_password,
                "spark.hive.metastore.truststore.path": "file:///opt/ibm/jdk/lib/security/cacerts",
                "spark.hive.metastore.truststore.password": "changeit",
                "spark.hive.metastore.truststore.type": "JKS",
"spark.sql.iceberg.commit.manifest.min-count-to-merge": "10",
"spark.sql.iceberg.commit.manifest.target-size-bytes": "8388608",

                # Timeouts
                "spark.hive.metastore.client.socket.timeout": "600",
                "spark.network.timeout": "600s",
                
                # Iceberg catalog - DON'T set .uri, let it use metastore.uris
                "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.iceberg_catalog.type": "hive",
                
                # Catalog implementation
                "spark.sql.catalogImplementation": "hive",
                
                # Extensions
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                
                # Iceberg settings
                
                # watsonx.data - uses different port
                "spark.hadoop.wxd.cas.endpoint": "https://c6285b64-3d32-4762-8a36-db2d1e18dad3.cise77rd04nf1e5p5s20.lakehouse.appdomain.cloud:30972/cas/v1/signature",
                "spark.hadoop.wxd.apikey": wxd_encoded_apikey,
                "spark.sql.catalog.iceberg_catalog.warehouse": f"s3a://wastonx-data-bucket/iceberg",

                # S3/COS - main bucket
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.sql.debug.maxToStringFields":"1000",
                f"spark.hadoop.fs.s3a.bucket.{BUCKET_NAME}.endpoint": COS_ENDPOINT,
                f"spark.hadoop.fs.s3a.bucket.{BUCKET_NAME}.access.key": COS_ACCESS_KEY,
                f"spark.hadoop.fs.s3a.bucket.{BUCKET_NAME}.secret.key": COS_SECRET_KEY,
                f"spark.hadoop.fs.s3a.bucket.{BUCKET_NAME}.connection.ssl.enabled": "true",
                f"spark.hadoop.fs.s3a.bucket.{BUCKET_NAME}.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.hive.metastore.failure.retries": "5",
                "spark.hadoop.hive.metastore.client.connect.retry.delay": "5s",
                "spark.hadoop.hive.metastore.client.socket.timeout": "900",

                # S3/COS - bronze bucket
                f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET_NAME}.endpoint": COS_ENDPOINT,
                f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET_NAME}.access.key": COS_ACCESS_KEY,
                f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET_NAME}.secret.key": COS_SECRET_KEY,
                f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET_NAME}.connection.ssl.enabled": "true",
                f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET_NAME}.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            },
        }
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "LhInstanceId": LH_INSTANCE_ID,
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()["id"]



def get_job_status(token, app_id):
    """Get status of submitted Spark job"""
    url = f"{INSTANCE_ROUTE}/lakehouse/api/v3/spark_engines/{SPARK_ENGINE_ID}/applications/{app_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "LhInstanceId": LH_INSTANCE_ID,
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def monitor_job(token, app_id, poll_interval=10, max_wait=600):
    """Monitor job until completion or timeout"""
    print(f"Monitoring Application ID: {app_id}")
    
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > max_wait:
            print(f"\n[WARNING] Timeout reached ({max_wait}s). Job may still be running.")
            break
            
        try:
            status_data = get_job_status(token, app_id)
            state = status_data.get("state", "Unknown")
            
            print(f"[{int(elapsed)}s] Status: {state}")
            
            if state == "finished":
                print(f"\n[SUCCESS] Job completed successfully!")
                print(f"\nJob Details:")
                print(f"  - State: {state}")
                print(f"  - Duration: {elapsed:.1f}s")
                break
            elif state in ["failed", "stopped", "killed"]:
                print(f"\n[ERROR] Job {state}!")
                print(f"\nJob Details:")
                print(f"  - State: {state}")
                if "state_details" in status_data:
                    print(f"  - Details: {status_data['state_details']}")
                break
            elif state in ["accepted", "running", "submitted"]:
                # Job is still processing
                time.sleep(poll_interval)
            else:
                print(f"  Unknown state: {state}")
                time.sleep(poll_interval)
                
        except Exception as e:
            print(f"Error checking status: {e}")
            time.sleep(poll_interval)


if __name__ == "__main__":
    try:
       
        print("IBM watsonx.data Spark Job Submission")
        
        token = get_iam_token()
        
        app_id = submit_job(token)
        
        print("Monitoring job status...")
        monitor_job(token, app_id)
        
    except requests.exceptions.HTTPError as e:
        print(f"\n[ERROR] HTTP Error: {e}")
        print(f"Response: {e.response.text if e.response else 'No response'}")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()