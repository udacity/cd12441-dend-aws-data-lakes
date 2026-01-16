"""
Exercise 3: Bronze Layer Organization - Structuring Raw Data
Lesson 1: Introduction to Data Lakes and Lakehouses

Learning Objectives:
- Understand bronze layer organization principles
- Implement proper directory structure for different data types
- Add metadata and partitioning strategies
- Compare organized vs unorganized bronze layers
- Prepare data for silver layer processing

Prerequisites:
- Python environment with pandas, boto3
- AWS credentials configured
- BUCKET_NAME environment variable set
- Exercise 1 & 2 completed (data in bronze)
"""

import pandas as pd
import boto3
import json
import time
import os
from io import BytesIO
from datetime import datetime

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'lakehouse-lesson1-student-123456789')
S3_BRONZE_BASE = 'bronze/'

print("="*70)
print("EXERCISE 3: BRONZE LAYER ORGANIZATION")
print("="*70)

# Initialize S3 client
print("\n[Step 1] Initializing AWS S3 Connection...")
s3_client = boto3.client('s3')
print("✓ S3 client initialized")

# Step 2: Review current bronze layer structure
print("\n[Step 2] Reviewing Current Bronze Layer Structure...")
print(f"  Current structure (from Exercises 1 & 2):")
print(f"""
  s3://{BUCKET_NAME}/bronze/
  ├── orders/orders.parquet         (Structured data)
  └── clickstream/clickstream.parquet (Unstructured data)
""")

print("  ⚠️  Issues with current structure:")
print("     • No partitioning (all data in one file)")
print("     • No metadata (ingestion time, source system)")
print("     • No data type separation (raw vs processed)")
print("     • Difficult to manage incremental loads")

# Step 3: Design organized bronze layer structure
print("\n[Step 3] Designing Organized Bronze Layer Structure...")
print("""
  Recommended bronze layer organization:

  s3://{bucket}/bronze/
  ├── structured/                    ← Data type separation
  │   └── orders/                    ← Source system
  │       ├── raw/                   ← Raw format preservation
  │       │   └── date=2025-01-15/   ← Date partitioning
  │       │       └── orders.parquet
  │       └── metadata.json          ← Dataset metadata
  │
  └── unstructured/                  ← Data type separation
      └── clickstream/               ← Source system
          ├── raw/                   ← Raw format preservation
          │   └── date=2025-01-15/   ← Date partitioning
          │       └── clickstream.parquet
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
response = s3_client.get_object(Bucket=BUCKET_NAME, Key='bronze/orders/orders.parquet')
orders_df = pd.read_parquet(BytesIO(response['Body'].read()))
print(f"  Loaded {len(orders_df):,} orders from existing bronze")

# Add metadata columns
orders_df['ingestion_timestamp'] = datetime.now()
orders_df['source_system'] = 'postgresql'
orders_df['data_type'] = 'structured'
orders_df['ingestion_date'] = pd.to_datetime(orders_df['order_date']).dt.date

# Group by date and write partitions
print(f"  Writing to organized structure with date partitions...")
start_time = time.time()

partition_count = 0
for date, group in orders_df.groupby('ingestion_date'):
    partition_path = f'bronze/structured/orders/raw/date={date}/orders.parquet'
    buffer = BytesIO()
    group.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=partition_path, Body=buffer.getvalue())
    partition_count += 1

write_time = time.time() - start_time
print(f"✓ Structured data reorganized in {write_time:.2f}s")
print(f"  Partitions created: {partition_count}")

# Step 5: Reorganize unstructured data (clickstream)
print("\n[Step 5] Reorganizing Unstructured Data (Clickstream)...")

# Read existing clickstream data
response = s3_client.get_object(Bucket=BUCKET_NAME, Key='bronze/clickstream/clickstream.parquet')
clickstream_df = pd.read_parquet(BytesIO(response['Body'].read()))
print(f"  Loaded {len(clickstream_df):,} events from existing bronze")

# Add metadata columns
clickstream_df['ingestion_timestamp'] = datetime.now()
clickstream_df['source_system'] = 'web_analytics'
clickstream_df['data_type'] = 'unstructured'
clickstream_df['ingestion_date'] = pd.to_datetime(clickstream_df['timestamp']).dt.date

# Group by date and write partitions
print(f"  Writing to organized structure with date partitions...")
start_time = time.time()

partition_count = 0
for date, group in clickstream_df.groupby('ingestion_date'):
    partition_path = f'bronze/unstructured/clickstream/raw/date={date}/clickstream.parquet'
    buffer = BytesIO()
    group.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=partition_path, Body=buffer.getvalue())
    partition_count += 1

write_time = time.time() - start_time
print(f"✓ Unstructured data reorganized in {write_time:.2f}s")
print(f"  Partitions created: {partition_count}")

# Step 6: Create metadata files
print("\n[Step 6] Creating Metadata Files...")

orders_metadata = {
    "dataset_name": "orders",
    "data_type": "structured",
    "source_system": "postgresql",
    "format": "parquet",
    "schema_version": "1.0",
    "partitioning": "date",
    "ingestion_frequency": "daily",
    "row_count": len(orders_df),
    "columns": len(orders_df.columns),
    "created_at": datetime.now().isoformat()
}

clickstream_metadata = {
    "dataset_name": "clickstream",
    "data_type": "unstructured",
    "source_system": "web_analytics",
    "format": "json_to_parquet",
    "schema_version": "1.0",
    "partitioning": "date",
    "ingestion_frequency": "hourly",
    "row_count": len(clickstream_df),
    "columns": len(clickstream_df.columns),
    "nested_fields": ["metadata"],
    "created_at": datetime.now().isoformat()
}

# Upload metadata files
s3_client.put_object(
    Bucket=BUCKET_NAME,
    Key='bronze/structured/orders/metadata.json',
    Body=json.dumps(orders_metadata, indent=2)
)
s3_client.put_object(
    Bucket=BUCKET_NAME,
    Key='bronze/unstructured/clickstream/metadata.json',
    Body=json.dumps(clickstream_metadata, indent=2)
)

print("✓ Metadata created:")
print(f"  Orders: {orders_metadata['row_count']:,} rows, {orders_metadata['columns']} columns")
print(f"  Clickstream: {clickstream_metadata['row_count']:,} events, {clickstream_metadata['columns']} columns")

# Step 7: Demonstrate partition benefits
print("\n[Step 7] Demonstrating Partition Benefits...")
sample_date = orders_df['ingestion_date'].iloc[0]
print(f"  Querying single partition: date={sample_date}")

start_time = time.time()
partition_path = f'bronze/structured/orders/raw/date={sample_date}/orders.parquet'
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=partition_path)
partition_df = pd.read_parquet(BytesIO(response['Body'].read()))
partition_time = time.time() - start_time

print(f"✓ Partition query completed in {partition_time:.2f}s")
print(f"  Rows in partition: {len(partition_df):,}")
print(f"  Benefit: Only reads relevant partition, not entire dataset")

# Step 8: Compare organized vs unorganized
print("\n[Step 8] Comparison: Organized vs Unorganized Bronze...")

print(f"""
  📁 UNORGANIZED (Exercises 1 & 2):
     Structure: Flat files
     └── bronze/orders/orders.parquet
     └── bronze/clickstream/clickstream.parquet
     
     Issues:
     ✗ No partitioning → Must read entire file
     ✗ No metadata → Unknown lineage
     ✗ No data type separation → Hard to manage
     ✗ No incremental load support

  📁 ORGANIZED (Exercise 3):
     Structure: Hierarchical with partitions
     └── bronze/structured/orders/raw/date=2025-01-15/orders.parquet
     └── bronze/unstructured/clickstream/raw/date=2025-01-15/clickstream.parquet
     
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
print(f"   • Structured data: s3://{BUCKET_NAME}/bronze/structured/orders/raw/")
print(f"   • Unstructured data: s3://{BUCKET_NAME}/bronze/unstructured/clickstream/raw/")
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

# Step 2: Review current bronze layer structure
print("\n[Step 2] Reviewing Current Bronze Layer Structure...")
print(f"  Current structure (from Exercises 1 & 2):")
print(f"""
  s3://{BUCKET_NAME}/bronze/
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

  s3://{bucket}/bronze/
  ├── structured/                    ← Data type separation
  │   └── orders/                    ← Source system
  │       ├── raw/                   ← Raw format preservation
  │       │   └── date=2025-01-15/   ← Date partitioning
  │       │       └── *.parquet
  │       └── metadata.json          ← Dataset metadata
  │
  └── unstructured/                  ← Data type separation
      └── clickstream/               ← Source system
          ├── raw/                   ← Raw format preservation
          │   └── date=2025-01-15/   ← Date partitioning
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
orders_df = spark.read.parquet(f'{S3_BRONZE_BASE}orders/')
print(f"  Loaded {orders_df.count():,} orders from existing bronze")

# Add metadata columns
orders_organized = orders_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("postgresql")) \
    .withColumn("data_type", lit("structured")) \
    .withColumn("ingestion_date", to_date(col("order_date")))

# Write with proper organization
organized_orders_path = f'{S3_BRONZE_BASE}structured/orders/raw/'
print(f"  Writing to organized structure: {organized_orders_path}")

start_time = time.time()
orders_organized.write \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .parquet(organized_orders_path)
write_time = time.time() - start_time

print(f"✓ Structured data reorganized in {write_time:.2f}s")
print(f"  Partitions created: {orders_organized.select('ingestion_date').distinct().count()}")

# Step 5: Reorganize unstructured data (clickstream)
print("\n[Step 5] Reorganizing Unstructured Data (Clickstream)...")

# Read existing clickstream data
clickstream_df = spark.read.parquet(f'{S3_BRONZE_BASE}clickstream/')
print(f"  Loaded {clickstream_df.count():,} events from existing bronze")

# Add metadata columns
clickstream_organized = clickstream_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("web_analytics")) \
    .withColumn("data_type", lit("unstructured")) \
    .withColumn("ingestion_date", to_date(col("timestamp")))

# Write with proper organization
organized_clickstream_path = f'{S3_BRONZE_BASE}unstructured/clickstream/raw/'
print(f"  Writing to organized structure: {organized_clickstream_path}")

start_time = time.time()
clickstream_organized.write \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .parquet(organized_clickstream_path)
write_time = time.time() - start_time

print(f"✓ Unstructured data reorganized in {write_time:.2f}s")
print(f"  Partitions created: {clickstream_organized.select('ingestion_date').distinct().count()}")

# Step 6: Create metadata files
print("\n[Step 6] Creating Metadata Files...")

orders_metadata = {
    "dataset_name": "orders",
    "data_type": "structured",
    "source_system": "postgresql",
    "format": "parquet",
    "schema_version": "1.0",
    "partitioning": "date",
    "ingestion_frequency": "daily",
    "row_count": orders_df.count(),
    "columns": len(orders_df.columns),
    "created_at": datetime.now().isoformat()
}

clickstream_metadata = {
    "dataset_name": "clickstream",
    "data_type": "unstructured",
    "source_system": "web_analytics",
    "format": "json_to_parquet",
    "schema_version": "1.0",
    "partitioning": "date",
    "ingestion_frequency": "hourly",
    "row_count": clickstream_df.count(),
    "columns": len(clickstream_df.columns),
    "nested_fields": ["metadata"],
    "created_at": datetime.now().isoformat()
}

print("✓ Metadata created:")
print(f"  Orders: {orders_metadata['row_count']:,} rows, {orders_metadata['columns']} columns")
print(f"  Clickstream: {clickstream_metadata['row_count']:,} events, {clickstream_metadata['columns']} columns")

# Step 7: Query organized data with partition pruning
print("\n[Step 7] Demonstrating Partition Pruning Benefits...")

# Query specific date partition
sample_date = orders_organized.select("ingestion_date").first()[0]
print(f"  Querying single partition: date={sample_date}")

start_time = time.time()
partition_query = spark.read.parquet(organized_orders_path) \
    .filter(col("ingestion_date") == sample_date) \
    .count()
partition_time = time.time() - start_time

print(f"✓ Partition query completed in {partition_time:.2f}s")
print(f"  Rows in partition: {partition_query:,}")
print(f"  Benefit: Only scans relevant partition, not entire dataset")

# Step 8: Compare organized vs unorganized
print("\n[Step 8] Comparison: Organized vs Unorganized Bronze...")

print(f"""
  📁 UNORGANIZED (Exercises 1 & 2):
     Structure: Flat directories
     └── bronze/orders/*.parquet
     └── bronze/clickstream/*.parquet
     
     Issues:
     ✗ No partitioning → Full table scans
     ✗ No metadata → Unknown lineage
     ✗ No data type separation → Hard to manage
     ✗ No incremental load support

  📁 ORGANIZED (Exercise 3):
     Structure: Hierarchical with partitions
     └── bronze/structured/orders/raw/date=2025-01-15/*.parquet
     └── bronze/unstructured/clickstream/raw/date=2025-01-15/*.parquet
     
     Benefits:
     ✓ Date partitioning → Efficient queries
     ✓ Metadata tracking → Clear lineage
     ✓ Data type separation → Easy management
     ✓ Incremental load ready → Append new partitions
""")

# Step 9: Best practices summary
print("\n[Step 9] Bronze Layer Organization Best Practices...")
print("""
  1. DATA TYPE SEPARATION
     • structured/ for tables with fixed schemas
     • unstructured/ for JSON, logs, semi-structured data
     
  2. SOURCE SYSTEM IDENTIFICATION
     • orders/ from PostgreSQL
     • clickstream/ from web analytics
     • Clear naming for data lineage
     
  3. PARTITIONING STRATEGY
     • Date partitioning (most common)
     • Enables partition pruning for faster queries
     • Supports incremental loads (append new dates)
     
  4. METADATA TRACKING
     • Schema version for evolution tracking
     • Ingestion timestamp for audit trail
     • Source system for lineage
     • Row counts for quality checks
     
  5. RAW FORMAT PRESERVATION
     • Keep original format in raw/ subdirectory
     • Enables reprocessing if needed
     • Audit trail for compliance
""")

# Summary
print("\n" + "="*70)
print("EXERCISE 3 SUMMARY")
print("="*70)
print(f"\n📊 Bronze Layer Organization:")
print(f"   • Structured data: {organized_orders_path}")
print(f"   • Unstructured data: {organized_clickstream_path}")
print(f"   • Partitioning: By ingestion_date")
print(f"   • Metadata: Tracked for both datasets")

print(f"\n✅ Key Improvements:")
print(f"   1. Data type separation (structured vs unstructured)")
print(f"   2. Date partitioning for efficient queries")
print(f"   3. Metadata tracking for lineage and quality")
print(f"   4. Incremental load support (append new partitions)")
print(f"   5. Clear directory structure for easy navigation")

print(f"\n🎯 Benefits for Silver Layer:")
print(f"   • Partition pruning reduces processing time")
print(f"   • Metadata enables quality checks")
print(f"   • Clear structure simplifies ETL pipelines")
print(f"   • Incremental processing becomes straightforward")

print("\n" + "="*70)
print("Next: Silver Layer - Clean & Transform Organized Data")
print("="*70)

spark.stop()
