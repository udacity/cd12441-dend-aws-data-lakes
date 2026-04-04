"""
Exercise 3: Bronze Layer Organization - Structuring Raw Data
Lesson 1: Introduction to Data Lakes and Lakehouses

Learning Objectives:
- Understand bronze layer organization principles
- Implement proper directory structure for different data types
- Add metadata and partitioning strategies
- Compare organized vs unorganized bronze layers
- Prepare data for silver layer processing

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run the script to organize bronze layer
3. Verify partitioned structure in S3
"""

import pandas as pd
import boto3
import json
import time
import os
from io import BytesIO
from datetime import datetime
from load_env import load_env

load_env()

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'lakehouse-lesson1-student-123456789')

print("="*70)
print("EXERCISE 3: BRONZE LAYER ORGANIZATION")
print("="*70)

# Initialize S3 client
print("\n[Step 1] Initializing AWS S3 Connection...")
# TODO: Initialize S3 client
s3_client = # YOUR CODE HERE
print("✓ S3 client initialized")

# Step 2: Review current bronze layer structure
print("\n[Step 2] Reviewing Current Bronze Layer Structure...")
print(f"  Current structure (from Exercises 1 & 2):")
print(f"""
  s3://{BUCKET_NAME}/
  ├── orders/              (Structured data - Parquet)
  │   └── *.parquet
  └── clickstream/         (Unstructured data - JSON → Parquet)
      └── *.parquet
""")

print("  ⚠️  Issues with current structure:")
print("     • No partitioning (all data in one directory)")
print("     • No metadata (ingestion time, source system)")
print("     • No data type separation (raw vs processed)")
print("     • Difficult to manage incremental loads")

# Step 3: Design organized bronze layer structure
print("\n[Step 3] Designing Organized Bronze Layer Structure...")
print("""
  Recommended bronze layer organization:

  s3://{bucket}/
  ├── structured/                    ← Data type separation
  │   └── orders/                    ← Source system
  │       ├── raw/                   ← Raw format preservation
  │       │   └── date=2026-01-15/   ← Date partitioning
  │       │       └── *.parquet
  │       └── metadata.json          ← Dataset metadata
  │
  └── unstructured/                  ← Data type separation
      └── clickstream/               ← Source system
          ├── raw/                   ← Raw format preservation
          │   └── date=2026-01-15/   ← Date partitioning
          │       └── *.parquet
          └── metadata.json          ← Dataset metadata

  Benefits:
  ✓ Clear data type separation (structured vs unstructured)
  ✓ Source system identification (orders, clickstream)
  ✓ Date partitioning for efficient queries
  ✓ Metadata tracking (schema, lineage, quality)
  ✓ Raw format preservation for audit trail
""")

# Step 4: Reorganize structured data (orders)
print("\n[Step 4] Reorganizing Structured Data (Orders)...")

# Read existing orders data
try:
    # TODO: List all parquet files in orders directory
    response = # YOUR CODE HERE
    if 'Contents' not in response:
        print("  ⚠️  No orders data found. Run Exercise 1 first.")
        orders_df = None
    else:
        # TODO: Read the first parquet file found
        orders_key = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')][0]
        response = # YOUR CODE HERE
        orders_df = # YOUR CODE HERE
        print(f"  Loaded {len(orders_df):,} orders from existing bronze")

        # TODO: Add metadata columns
        orders_df['ingestion_timestamp'] = # YOUR CODE HERE
        orders_df['source_system'] = # YOUR CODE HERE
        orders_df['data_type'] = # YOUR CODE HERE
        orders_df['ingestion_date'] = # YOUR CODE HERE (convert order_date to date)

        # Group by date and write partitions
        print(f"  Writing to organized structure with date partitions...")
        start_time = time.time()

        partition_count = 0
        for date, group in orders_df.groupby('ingestion_date'):
            partition_path = f'structured/orders/raw/date={date}/orders.parquet'
            # TODO: Write partition to S3
            buffer = BytesIO()
            # YOUR CODE HERE
            buffer.seek(0)
            # YOUR CODE HERE
            partition_count += 1

        write_time = time.time() - start_time
        print(f"✓ Structured data reorganized in {write_time:.2f}s")
        print(f"  Partitions created: {partition_count}")
except Exception as e:
    print(f"  ⚠️  Error reorganizing orders: {e}")
    orders_df = None

# Step 5: Reorganize unstructured data (clickstream)
print("\n[Step 5] Reorganizing Unstructured Data (Clickstream)...")

try:
    # TODO: List all parquet files in clickstream directory
    response = # YOUR CODE HERE
    if 'Contents' not in response:
        print("  ⚠️  No clickstream data found. Run Exercise 2 first.")
        clickstream_df = None
    else:
        # TODO: Read the first parquet file found
        clickstream_key = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')][0]
        response = # YOUR CODE HERE
        clickstream_df = # YOUR CODE HERE
        print(f"  Loaded {len(clickstream_df):,} events from existing bronze")

        # TODO: Add metadata columns
        clickstream_df['ingestion_timestamp'] = # YOUR CODE HERE
        clickstream_df['source_system'] = # YOUR CODE HERE
        clickstream_df['data_type'] = # YOUR CODE HERE
        clickstream_df['ingestion_date'] = # YOUR CODE HERE (convert timestamp to date)

        # Group by date and write partitions
        print(f"  Writing to organized structure with date partitions...")
        start_time = time.time()

        partition_count = 0
        for date, group in clickstream_df.groupby('ingestion_date'):
            partition_path = f'unstructured/clickstream/raw/date={date}/clickstream.parquet'
            # TODO: Write partition to S3
            buffer = BytesIO()
            # YOUR CODE HERE
            buffer.seek(0)
            # YOUR CODE HERE
            partition_count += 1

        write_time = time.time() - start_time
        print(f"✓ Unstructured data reorganized in {write_time:.2f}s")
        print(f"  Partitions created: {partition_count}")
except Exception as e:
    print(f"  ⚠️  Error reorganizing clickstream: {e}")
    clickstream_df = None

# Step 6: Create metadata files
print("\n[Step 6] Creating Metadata Files...")

if orders_df is not None:
    # TODO: Create orders metadata dictionary
    orders_metadata = {
        "dataset_name": "orders",
        "data_type": "structured",
        "source_system": "postgresql",
        "format": "parquet",
        "schema_version": "1.0",
        "partitioning": "date",
        "ingestion_frequency": "daily",
        "row_count": # YOUR CODE HERE,
        "columns": # YOUR CODE HERE,
        "created_at": # YOUR CODE HERE
    }
    # TODO: Upload metadata to S3
    # YOUR CODE HERE
    
    print(f"✓ Orders metadata: {orders_metadata['row_count']:,} rows, {orders_metadata['columns']} columns")

if clickstream_df is not None:
    # TODO: Create clickstream metadata dictionary
    clickstream_metadata = {
        "dataset_name": "clickstream",
        "data_type": "unstructured",
        "source_system": "web_analytics",
        "format": "json_to_parquet",
        "schema_version": "1.0",
        "partitioning": "date",
        "ingestion_frequency": "hourly",
        "row_count": # YOUR CODE HERE,
        "columns": # YOUR CODE HERE,
        "nested_fields": ["metadata"],
        "created_at": # YOUR CODE HERE
    }
    # TODO: Upload metadata to S3
    # YOUR CODE HERE
    
    print(f"✓ Clickstream metadata: {clickstream_metadata['row_count']:,} events, {clickstream_metadata['columns']} columns")

# Step 7: Demonstrate partition benefits
print("\n[Step 7] Demonstrating Partition Benefits...")
if orders_df is not None:
    sample_date = orders_df['ingestion_date'].iloc[0]
    print(f"  Querying single partition: date={sample_date}")

    start_time = time.time()
    partition_path = f'structured/orders/raw/date={sample_date}/orders.parquet'
    # TODO: Read partition from S3
    response = # YOUR CODE HERE
    partition_df = # YOUR CODE HERE
    partition_time = time.time() - start_time

    print(f"✓ Partition query completed in {partition_time:.2f}s")
    print(f"  Rows in partition: {len(partition_df):,}")
    print(f"  Benefit: Only reads relevant partition, not entire dataset")

# Step 8: Compare organized vs unorganized
print("\n[Step 8] Comparison: Organized vs Unorganized Bronze...")

print(f"""
  📁 UNORGANIZED (Exercises 1 & 2):
     Structure: Flat files
     └── orders/orders.parquet
     └── clickstream/clickstream.parquet
     
     Issues:
     ✗ No partitioning → Must read entire file
     ✗ No metadata → Unknown lineage
     ✗ No data type separation → Hard to manage
     ✗ No incremental load support

  📁 ORGANIZED (Exercise 3):
     Structure: Hierarchical with partitions
     └── structured/orders/raw/date=2026-01-15/orders.parquet
     └── unstructured/clickstream/raw/date=2026-01-15/clickstream.parquet
     
     Benefits:
     ✓ Date partitioning → Efficient queries
     ✓ Metadata tracking → Clear lineage
     ✓ Data type separation → Easy management
     ✓ Incremental load ready → Append new partitions
""")

# Summary
print("\n" + "="*70)
print("EXERCISE 3 SUMMARY")
print("="*70)
print(f"\n📊 Bronze Layer Organization:")
print(f"   • Structured data: s3://{BUCKET_NAME}/structured/orders/raw/")
print(f"   • Unstructured data: s3://{BUCKET_NAME}/unstructured/clickstream/raw/")
print(f"   • Partitioning: By ingestion_date")
print(f"   • Metadata: Tracked for both datasets")

print(f"\n✅ Key Improvements:")
print(f"   1. Data type separation (structured vs unstructured)")
print(f"   2. Date partitioning for efficient queries")
print(f"   3. Metadata tracking for lineage and quality")
print(f"   4. Incremental load support (append new partitions)")
print(f"   5. Clear directory structure for easy navigation")

print(f"\n🎯 Benefits for Future Processing:")
print(f"   • Partition pruning reduces processing time")
print(f"   • Metadata enables quality checks")
print(f"   • Clear structure simplifies ETL pipelines")
print(f"   • Incremental processing becomes straightforward")

print("\n" + "="*70)
print("Lesson 1 Complete - Bronze Layer Organized!")
print("="*70)
