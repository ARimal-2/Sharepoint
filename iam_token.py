import requests
import os

def get_iam_token(api_key: str) -> str:
    url = "https://iam.cloud.ibm.com/identity/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = (
        "grant_type=urn:ibm:params:oauth:grant-type:apikey"
        f"&apikey={api_key}"
    )
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

iam_token = get_iam_token(API_KEY)
print("IAM Token:", iam_token)