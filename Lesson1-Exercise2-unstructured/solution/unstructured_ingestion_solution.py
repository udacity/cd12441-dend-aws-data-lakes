"""
Exercise 2: Bronze Layer - Unstructured Data Ingestion
Lesson 1: Introduction to Data Lakes and Lakehouses

Learning Objectives:
- Understand schema-on-read with unstructured data (JSON)
- Load nested JSON data using pandas
- Compare structured (Parquet) vs unstructured (JSON) ingestion
- Observe schema flexibility and inference

Prerequisites:
- Python environment with pandas, boto3
- AWS credentials configured
- BUCKET_NAME environment variable set
- Exercise 1 completed (structured data in bronze)
"""

import pandas as pd
import boto3
import json
import time
import os
import uuid
from io import BytesIO

from load_env import load_env

load_env()

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME', f'lakehouse-student-bronze-{uuid.uuid4()}')

s3_client = boto3.client("s3")
#Verify if bucket already exists
buckets = s3_client.list_buckets(
    MaxBuckets=1,
    Prefix="lakehouse-student-bronze-",
    BucketRegion='us-east-1'
)['Buckets']

# Get name of bucket if already exists
if buckets:
    BUCKET_NAME = buckets[0]['Name']

LOCAL_DATA_PATH = 'data/clickstream.json'
S3_BRONZE_PATH = 'clickstream/clickstream.parquet'

print("="*70)
print("EXERCISE 2: BRONZE LAYER - UNSTRUCTURED DATA INGESTION")
print("="*70)

# Initialize S3 client
print("\n[Step 1] Initializing AWS S3 Connection...")
s3_client = boto3.client('s3')
print("✓ S3 client initialized")

# Step 2: Load unstructured data (JSON)
print("\n[Step 2] Loading unstructured data (clickstream.json)...")
print(f"  Source: {LOCAL_DATA_PATH}")
print("  Format: JSON Lines (one JSON object per line)")

start_time = time.time()
clickstream_df = pd.read_json(LOCAL_DATA_PATH, lines=True)
load_time = time.time() - start_time

print(f"✓ Data loaded in {load_time:.2f} seconds")
print(f"  Events: {len(clickstream_df):,}")
print(f"  Columns: {len(clickstream_df.columns)}")

# Step 3: Examine inferred schema (schema-on-read)
print("\n[Step 3] Examining Schema (Schema-on-Read)...")
print("  JSON schema is INFERRED at read time - flexible structure:")
print(f"\n{clickstream_df.dtypes.to_string()}")

# Step 4: Preview data with nested structure
print("\n[Step 4] Preview Sample Data (Nested JSON)...")
print(clickstream_df.head(5).to_string())

# Step 5: Explore nested metadata
print("\n[Step 5] Exploring Nested Structure...")
print("  JSON contains nested 'metadata' object:")
print("\n  Sample metadata:")
for idx in range(min(3, len(clickstream_df))):
    print(f"    Event {idx+1}: {clickstream_df.iloc[idx]['metadata']}")

print("\n  Accessing nested fields:")
# Expand nested metadata
metadata_df = pd.json_normalize(clickstream_df['metadata'])
clickstream_expanded = clickstream_df.copy()
clickstream_expanded['browser'] = metadata_df['browser']
clickstream_expanded['device'] = metadata_df['device']
clickstream_expanded['referrer'] = metadata_df['referrer']

print(clickstream_expanded[['event_id', 'event_type', 'browser', 'device', 'referrer']].head(5).to_string())

# Step 6: Schema flexibility - variable fields
print("\n[Step 6] Schema Flexibility - Variable Fields...")
print("  Not all events have 'product_id' (only product-related events):")

with_product = clickstream_df['product_id'].notna().sum()
without_product = clickstream_df['product_id'].isna().sum()
total = len(clickstream_df)

print(f"\n  Events with product_id: {with_product:,} ({with_product/total*100:.1f}%)")
print(f"  Events without product_id: {without_product:,} ({without_product/total*100:.1f}%)")
print("\n  ✓ Schema-on-read handles variable fields gracefully (nulls for missing)")

# Step 7: Write to S3 bronze layer
print(f"\n[Step 7] Writing to S3 Bronze Layer...")
print(f"  Destination: s3://{BUCKET_NAME}/{S3_BRONZE_PATH}")
print("  Strategy: Append-only (raw data preservation)")
print("  Format: Parquet (for efficient querying, but preserving JSON structure)")

start_time = time.time()
buffer = BytesIO()
clickstream_df.to_parquet(buffer, index=False)
buffer.seek(0)
s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH, Body=buffer.getvalue())
write_time = time.time() - start_time

print(f"✓ Data written to S3 in {write_time:.2f} seconds")

# Step 8: Verify S3 write
print("\n[Step 8] Verifying S3 Bronze Layer...")
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH)
bronze_df = pd.read_parquet(BytesIO(response['Body'].read()))
s3_row_count = len(bronze_df)

print(f"✓ Verification successful")
print(f"  Events in S3: {s3_row_count:,}")
print(f"  Match: {'✓' if s3_row_count == total else '✗'}")

# Step 9: Compare with Exercise 1 (Structured Data)
print("\n[Step 9] Comparison: Structured vs Unstructured...")
print("\n  Loading Exercise 1 data (orders.parquet) for comparison...")

orders_path = 'orders/orders.parquet'
try:
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=orders_path)
    orders_df = pd.read_parquet(BytesIO(response['Body'].read()))
    orders_count = len(orders_df)
    
    print(f"\n  📊 STRUCTURED DATA (Orders - Parquet):")
    print(f"     • Schema: Explicit, pre-defined")
    print(f"     • Load time: Fast (columnar format)")
    print(f"     • Rows: {orders_count:,}")
    print(f"     • Columns: {len(orders_df.columns)} (flat structure)")
    print(f"     • Flexibility: Low (rigid schema)")
    
    print(f"\n  📊 UNSTRUCTURED DATA (Clickstream - JSON):")
    print(f"     • Schema: Inferred at read time")
    print(f"     • Load time: {load_time:.2f}s (schema inference overhead)")
    print(f"     • Events: {total:,}")
    print(f"     • Columns: {len(clickstream_df.columns)} (nested structure)")
    print(f"     • Flexibility: High (variable fields, nested objects)")
    
except Exception as e:
    print(f"  ⚠️  Could not load Exercise 1 data: {e}")
    print(f"     Run Exercise 1 first to enable comparison")

# Summary
print("\n" + "="*70)
print("EXERCISE 2 SUMMARY")
print("="*70)
print(f"\n📊 Unstructured Data (JSON):")
print(f"   • Schema: Inferred at read time (schema-on-read)")
print(f"   • Load time: {load_time:.2f}s")
print(f"   • Write time: {write_time:.2f}s")
print(f"   • Total events: {total:,}")
print(f"   • Nested structure: metadata object with browser/device/referrer")
print(f"   • Variable fields: product_id present in {with_product/total*100:.1f}% of events")

print(f"\n🗂️  Bronze Layer Characteristics:")
print(f"   • Raw data preserved as-is")
print(f"   • Flexible schema (handles nested + variable fields)")
print(f"   • Append-only strategy")
print(f"   • Location: s3://{BUCKET_NAME}/{S3_BRONZE_PATH}")

print(f"\n✅ Key Takeaway:")
print(f"   Unstructured data (JSON) uses schema-on-read - schema is inferred")
print(f"   when reading, not enforced when writing. This enables flexibility")
print(f"   for nested structures and variable fields, but requires careful")
print(f"   handling of schema evolution and data quality.")

