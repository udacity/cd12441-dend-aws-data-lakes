"""
Exercise 2: Bronze Layer - Unstructured Data Ingestion
Lesson 1: Introduction to Data Lakes and Lakehouses

Learning Objectives:
- Understand schema-on-read with unstructured data (JSON)
- Load nested JSON data using pandas
- Compare structured (Parquet) vs unstructured (JSON) ingestion
- Observe schema flexibility and inference

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run the script and observe schema inference
3. Compare with Exercise 1 structured approach
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
LOCAL_DATA_PATH = 'data/clickstream.json'
S3_BRONZE_PATH = 'clickstream/clickstream.parquet'

print("="*70)
print("EXERCISE 2: BRONZE LAYER - UNSTRUCTURED DATA INGESTION")
print("="*70)

# Initialize S3 client
print("\n[Step 1] Initializing AWS S3 Connection...")
# TODO: Initialize S3 client
s3_client = # YOUR CODE HERE
print("✓ S3 client initialized")

# Step 2: Load unstructured data (JSON)
print("\n[Step 2] Loading unstructured data (clickstream.json)...")
print(f"  Source: {LOCAL_DATA_PATH}")
print("  Format: JSON Lines (one JSON object per line)")

start_time = time.time()
# TODO: Load JSON Lines file using pandas
clickstream_df = # YOUR CODE HERE
load_time = time.time() - start_time

print(f"✓ Data loaded in {load_time:.2f} seconds")
print(f"  Events: {len(clickstream_df):,}")
print(f"  Columns: {len(clickstream_df.columns)}")

# Step 3: Examine inferred schema (schema-on-read)
print("\n[Step 3] Examining Schema (Schema-on-Read)...")
print("  JSON schema is INFERRED at read time - flexible structure:")
# TODO: Print data types
print(f"\n{# YOUR CODE HERE}")

# Step 4: Preview data with nested structure
print("\n[Step 4] Preview Sample Data (Nested JSON)...")
# TODO: Print first 5 rows
print(# YOUR CODE HERE)

# Step 5: Explore nested metadata
print("\n[Step 5] Exploring Nested Structure...")
print("  JSON contains nested 'metadata' object:")
print("\n  Sample metadata:")
for idx in range(min(3, len(clickstream_df))):
    print(f"    Event {idx+1}: {clickstream_df.iloc[idx]['metadata']}")

print("\n  Accessing nested fields:")
# TODO: Expand nested metadata using json_normalize
metadata_df = # YOUR CODE HERE
clickstream_expanded = clickstream_df.copy()
# TODO: Add browser, device, referrer columns from metadata_df
clickstream_expanded['browser'] = # YOUR CODE HERE
clickstream_expanded['device'] = # YOUR CODE HERE
clickstream_expanded['referrer'] = # YOUR CODE HERE

print(clickstream_expanded[['event_id', 'event_type', 'browser', 'device', 'referrer']].head(5).to_string())

# Step 6: Schema flexibility - variable fields
print("\n[Step 6] Schema Flexibility - Variable Fields...")
print("  Not all events have 'product_id' (only product-related events):")

# TODO: Count events with and without product_id
with_product = # YOUR CODE HERE
without_product = # YOUR CODE HERE
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
print(f"  Events in S3: {s3_row_count:,}")
print(f"  Match: {'✓' if s3_row_count == total else '✗'}")

# Step 9: Compare with Exercise 1 (Structured Data)
print("\n[Step 9] Comparison: Structured vs Unstructured...")
print("\n  Loading Exercise 1 data (orders.parquet) for comparison...")

orders_path = 'orders/orders.parquet'
try:
    # TODO: Load orders data from S3
    response = # YOUR CODE HERE
    orders_df = # YOUR CODE HERE
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
