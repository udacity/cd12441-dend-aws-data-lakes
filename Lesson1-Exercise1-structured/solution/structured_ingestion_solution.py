"""
Exercise 1: Bronze Layer - Structured Data Ingestion
Lesson 1: Introduction to Data Lakes and Lakehouses

Learning Objectives:
- Understand schema-on-write with structured data (Parquet)
- Load data into bronze layer (raw, as-is ingestion)
- Compare local vs S3 storage
- Observe data quality issues in raw data

Prerequisites:
- Python environment with pandas, pyarrow, boto3
- AWS credentials configured
- BUCKET_NAME environment variable set
"""

import pandas as pd
import boto3
import time
import os
import uuid
from io import BytesIO
from load_env import load_env

load_env()

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME', f'lakehouse-student-bronze-{uuid.uuid4()}')
LOCAL_DATA_PATH = 'data/orders.parquet'
S3_BRONZE_PATH = f'orders/orders.parquet'

print("="*70)
print("EXERCISE 1: BRONZE LAYER - STRUCTURED DATA INGESTION")
print("="*70)

# Initialize S3 client
print("\n[Step 1] Initializing AWS S3 Connection...")
s3_client = boto3.client('s3')
print("✓ S3 client initialized")

# Step 2: Load structured data (Parquet)
print("\n[Step 2] Loading structured data (orders.parquet)...")
print(f"  Source: {LOCAL_DATA_PATH}")
print("  Format: Parquet (columnar, schema-on-write)")

start_time = time.time()
orders_df = pd.read_parquet(LOCAL_DATA_PATH)
load_time = time.time() - start_time

print(f"✓ Data loaded in {load_time:.2f} seconds")
print(f"  Rows: {len(orders_df):,}")
print(f"  Columns: {len(orders_df.columns)}")

# Step 3: Examine schema (schema-on-write)
print("\n[Step 3] Examining Schema (Schema-on-Write)...")
print("  Parquet enforces schema at write time - explicit data types:")
print(f"\n{orders_df.dtypes.to_string()}")

# Step 4: Preview data
print("\n[Step 4] Preview Sample Data...")
print(orders_df.head(5).to_string())

# Step 5: Data quality assessment (bronze = raw, as-is)
print("\n[Step 5] Data Quality Assessment (Raw Bronze Layer)...")
print("  Bronze layer contains data AS-IS with quality issues:")

total_rows = len(orders_df)
null_values = orders_df['order_value'].isna().sum()
duplicates = orders_df.duplicated(subset=['order_id']).sum()

print(f"\n  Total rows: {total_rows:,}")
print(f"  Null order_value: {null_values:,} ({null_values/total_rows*100:.1f}%)")
print(f"  Duplicate order_ids: {duplicates:,} ({duplicates/total_rows*100:.1f}%)")
print("\n  ⚠️  These issues will be cleaned in later exercises (Silver layer)")

# Step 6: Create bucket in S3

print("\n[Step 6] Create a new bucket for bronze layer ")

# Verify if bucket already exists
buckets = s3_client.list_buckets(
    MaxBuckets=1,
    Prefix="lakehouse-student-bronze-",
    BucketRegion='us-east-1'
)['Buckets']

# Get name of bucket if already exists
if buckets:
    BUCKET_NAME = buckets[0]['Name']

try:
    s3_client.create_bucket(
        ACL='private',
        Bucket=BUCKET_NAME
    )

    print(f"S3 Bucket created: {BUCKET_NAME}")
except s3_client.exceptions.BucketAlreadyExists:
    print(f"S3 Bucket already exists: {BUCKET_NAME}")
except Exception as e:
    print(f"Error: {e}")

# Step 7: Write to S3 bronze layer
print(f"\n[Step 7] Writing to S3 Bronze Layer...")
print(f"  Destination: s3://{BUCKET_NAME}/{S3_BRONZE_PATH}")
print("  Strategy: Append-only (raw data preservation)")

start_time = time.time()
# Write to buffer first, then upload to S3
buffer = BytesIO()
orders_df.to_parquet(buffer, index=False, coerce_timestamps='ms', allow_truncated_timestamps=True)
buffer.seek(0)
s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH, Body=buffer.getvalue())
write_time = time.time() - start_time

print(f"✓ Data written to S3 in {write_time:.2f} seconds")

# Step 8: Verify S3 write
print("\n[Step 8] Verifying S3 Bronze Layer...")
# Read back from S3
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH)
bronze_df = pd.read_parquet(BytesIO(response['Body'].read()))
s3_row_count = len(bronze_df)

print(f"✓ Verification successful")
print(f"  Rows in S3: {s3_row_count:,}")
print(f"  Match: {'✓' if s3_row_count == total_rows else '✗'}")

# Summary
print("\n" + "="*70)
print("EXERCISE 1 SUMMARY")
print("="*70)
print(f"\n📊 Structured Data (Parquet):")
print(f"   • Schema: Explicit, enforced at write time")
print(f"   • Load time: {load_time:.2f}s")
print(f"   • Write time: {write_time:.2f}s")
print(f"   • Total rows: {total_rows:,}")
print(f"\n🗂️  Bronze Layer Characteristics:")
print(f"   • Raw data preserved as-is")
print(f"   • Contains quality issues (nulls, duplicates)")
print(f"   • Append-only strategy")
print(f"   • Location: s3://{BUCKET_NAME}/{S3_BRONZE_PATH}")
print(f"\n✅ Key Takeaway:")
print(f"   Structured data (Parquet) has explicit schema enforced at write time,")
print(f"   enabling fast, type-safe queries. Bronze layer stores raw data with")
print(f"   all quality issues intact for downstream cleaning.")

