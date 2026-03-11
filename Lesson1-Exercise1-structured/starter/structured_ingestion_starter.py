"""
Exercise 1: Bronze Layer - Structured Data Ingestion

Learning Objectives:
- Understand schema-on-write with structured data (Parquet)
- Load data into bronze layer (raw, as-is ingestion)
- Compare local vs S3 storage
- Observe data quality issues in raw data

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run the script and verify outputs
3. Check S3 bronze layer for saved data
"""

import pandas as pd
import boto3
import time
import os
import uuid
from io import BytesIO

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME', f'lakehouse-student-bronze-{uuid.uuid4()}')
LOCAL_DATA_PATH = 'data/orders.parquet'
S3_BRONZE_PATH = f'orders/orders.parquet'

print("="*70)
print("EXERCISE 1: BRONZE LAYER - STRUCTURED DATA INGESTION")
print("="*70)

# Initialize S3 client
print("\n[Step 1] Initializing AWS S3 Connection...")
# TODO: Initialize S3 client
s3_client = # YOUR CODE HERE
print("✓ S3 client initialized")

# Step 2: Load structured data (Parquet)
print("\n[Step 2] Loading structured data (orders.parquet)...")
print(f"  Source: {LOCAL_DATA_PATH}")
print("  Format: Parquet (columnar, schema-on-write)")

start_time = time.time()
# TODO: Load parquet file using pandas
orders_df = # YOUR CODE HERE
load_time = time.time() - start_time

print(f"✓ Data loaded in {load_time:.2f} seconds")
print(f"  Rows: {len(orders_df):,}")
print(f"  Columns: {len(orders_df.columns)}")

# Step 3: Examine schema (schema-on-write)
print("\n[Step 3] Examining Schema (Schema-on-Write)...")
print("  Parquet enforces schema at write time - explicit data types:")
# TODO: Print data types
print(f"\n{# YOUR CODE HERE}")

# Step 4: Preview data
print("\n[Step 4] Preview Sample Data...")
# TODO: Print first 5 rows
print(# YOUR CODE HERE)

# Step 5: Data quality assessment (bronze = raw, as-is)
print("\n[Step 5] Data Quality Assessment (Raw Bronze Layer)...")
print("  Bronze layer contains data AS-IS with quality issues:")

total_rows = len(orders_df)
# TODO: Count null values in order_value column
null_values = # YOUR CODE HERE
# TODO: Count duplicate order_ids
duplicates = # YOUR CODE HERE

print(f"\n  Total rows: {total_rows:,}")
print(f"  Null order_value: {null_values:,} ({null_values/total_rows*100:.1f}%)")
print(f"  Duplicate order_ids: {duplicates:,} ({duplicates/total_rows*100:.1f}%)")
print("\n  ⚠️  These issues will be cleaned in later exercises (Silver layer)")

# Step 6: Create bucket in S3
print("\n[Step 6] Create a new bucket for bronze layer ")

# TODO: List existing buckets with prefix
buckets = # YOUR CODE HERE

# Get name of bucket if already exists
if buckets:
    BUCKET_NAME = buckets[0]['Name']

try:
    # TODO: Create S3 bucket
    # YOUR CODE HERE
    
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
# TODO: Write DataFrame to buffer as parquet
buffer = BytesIO()
# YOUR CODE HERE
buffer.seek(0)
# TODO: Upload to S3
# YOUR CODE HERE
write_time = time.time() - start_time

print(f"✓ Data written to S3 in {write_time:.2f} seconds")

# Step 8: Verify S3 write
print("\n[Step 8] Verifying S3 Bronze Layer...")
# TODO: Read back from S3
response = # YOUR CODE HERE
bronze_df = # YOUR CODE HERE
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

