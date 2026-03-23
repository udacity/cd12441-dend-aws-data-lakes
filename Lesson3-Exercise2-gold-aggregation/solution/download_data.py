"""Download gold aggregation data from S3"""
import boto3
import os

s3 = boto3.client('s3')
BUCKET = 'lakehouse-student-bronze-ae1254b1-cafe-479b-bce5-acb312f6d1c0'
PREFIX = 'gold-aggregation/data/'
LOCAL_DIR = 'data'

paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get('Contents', []):
        key = obj['Key']
        local_path = os.path.join(LOCAL_DIR, key[len(PREFIX):])
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if not key.endswith('/'):
            s3.download_file(BUCKET, key, local_path)
            print(f"✓ {local_path}")

print("\nDone.")
