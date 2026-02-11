import boto3
import os
import json

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ["R2_ACCESS_KEY"],
    aws_secret_access_key=os.environ["R2_SECRET_KEY"],
    region_name="auto"
)

data = {
    "status": "sistem calisti",
    "number": 123
}

with open("test.json", "w") as f:
    json.dump(data, f)

s3.upload_file(
    "test.json",
    os.environ["R2_BUCKET_NAME"],
    "public/test.json"
)

print("Upload tamam")
