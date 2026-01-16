"""
Exercise 3: Bronze Layer - Organization with Partitioning
Student Name: _______________
Date: _______________

Learning Objectives:
- Organize bronze layer with hierarchical structure
- Implement date partitioning for efficient queries
- Track metadata for data lineage
- Compare structured vs unstructured organization

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run the script to organize bronze layer
3. Verify partitioned structure in S3
"""

import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')
bronze_bucket = 'swiftshop-lakehouse'

print("=== EXERCISE 3: BRONZE LAYER ORGANIZATION ===\n")

# Step 1: Load Data from Bronze
print("STEP 1: LOAD DATA FROM BRONZE LAYER")
print("-" * 50)

# TODO: Load orders from bronze
orders_obj = s3.get_object(Bucket=bronze_bucket, Key='bronze/structured/orders/orders.parquet')
orders_df = # YOUR CODE HERE

# TODO: Load clickstream from bronze
clickstream_obj = s3.get_object(Bucket=bronze_bucket, Key='bronze/unstructured/clickstream/clickstream.parquet')
clickstream_df = # YOUR CODE HERE

print(f"Orders loaded: {len(orders_df)} records")
print(f"Clickstream loaded: {len(clickstream_df)} records\n")

# Step 2: Add Date Partition Column
print("STEP 2: ADD DATE PARTITION COLUMNS")
print("-" * 50)

# TODO: Extract date from order_date for partitioning
orders_df['partition_date'] = # YOUR CODE HERE (Hint: pd.to_datetime().dt.date)

# TODO: Extract date from timestamp for partitioning
clickstream_df['partition_date'] = # YOUR CODE HERE

print("Partition columns added:")
print(f"Orders date range: {orders_df['partition_date'].min()} to {orders_df['partition_date'].max()}")
print(f"Clickstream date range: {clickstream_df['partition_date'].min()} to {clickstream_df['partition_date'].max()}")
print(f"Unique dates in orders: {orders_df['partition_date'].nunique()}")
print(f"Unique dates in clickstream: {clickstream_df['partition_date'].nunique()}\n")

# Step 3: Save with Partitioning
print("STEP 3: SAVE WITH DATE PARTITIONING")
print("-" * 50)

# TODO: Group orders by partition_date and save each partition
orders_partitions_saved = 0
for date, group in orders_df.groupby('partition_date'):
    # TODO: Create partition key with date
    partition_key = f'bronze/structured/orders/date={date}/orders.parquet'
    
    # TODO: Save partition to S3
    parquet_buffer = BytesIO()
    # YOUR CODE HERE - save group to parquet_buffer
    
    # YOUR CODE HERE - upload to S3
    
    orders_partitions_saved += 1

print(f"✓ Orders saved to {orders_partitions_saved} date partitions")

# TODO: Group clickstream by partition_date and save each partition
clickstream_partitions_saved = 0
for date, group in clickstream_df.groupby('partition_date'):
    # TODO: Create partition key with date
    partition_key = f'bronze/unstructured/clickstream/date={date}/clickstream.parquet'
    
    # TODO: Save partition to S3
    parquet_buffer = BytesIO()
    # YOUR CODE HERE
    
    # YOUR CODE HERE
    
    clickstream_partitions_saved += 1

print(f"✓ Clickstream saved to {clickstream_partitions_saved} date partitions\n")

# Step 4: Verify Partitioned Structure
print("STEP 4: VERIFY PARTITIONED STRUCTURE")
print("-" * 50)

# TODO: List objects in bronze/structured/orders/ to see partitions
orders_response = s3.list_objects_v2(Bucket=bronze_bucket, Prefix='bronze/structured/orders/date=')
orders_partitions = [obj['Key'] for obj in orders_response.get('Contents', [])]

# TODO: List objects in bronze/unstructured/clickstream/ to see partitions
clickstream_response = s3.list_objects_v2(Bucket=bronze_bucket, Prefix='bronze/unstructured/clickstream/date=')
clickstream_partitions = [obj['Key'] for obj in clickstream_response.get('Contents', [])]

print("Bronze Layer Structure:")
print(f"bronze/")
print(f"├── structured/")
print(f"│   └── orders/")
print(f"│       └── date=YYYY-MM-DD/ ({len(orders_partitions)} partitions)")
print(f"└── unstructured/")
print(f"    └── clickstream/")
print(f"        └── date=YYYY-MM-DD/ ({len(clickstream_partitions)} partitions)\n")

# Step 5: Query Performance with Partitioning
print("STEP 5: PARTITION PRUNING BENEFIT")
print("-" * 50)

# TODO: Calculate data size for a single date partition
sample_date = orders_df['partition_date'].iloc[0]
single_partition = orders_df[orders_df['partition_date'] == sample_date]
all_data_size = len(orders_df)
single_partition_size = len(single_partition)

# TODO: Calculate efficiency gain
efficiency_gain = # YOUR CODE HERE (Hint: all_data_size / single_partition_size)

print(f"Sample query: Orders for {sample_date}")
print(f"Without partitioning: Scan {all_data_size} records")
print(f"With partitioning: Scan {single_partition_size} records")
print(f"Efficiency gain: {efficiency_gain:.1f}x faster\n")

# Step 6: Metadata Summary
print("STEP 6: BRONZE LAYER METADATA SUMMARY")
print("-" * 50)

# TODO: Create metadata summary
metadata_summary = {
    'structured_records': len(orders_df),
    'unstructured_records': len(clickstream_df),
    'structured_partitions': orders_partitions_saved,
    'unstructured_partitions': clickstream_partitions_saved,
    'date_range_start': min(orders_df['partition_date'].min(), clickstream_df['partition_date'].min()),
    'date_range_end': max(orders_df['partition_date'].max(), clickstream_df['partition_date'].max()),
    'ingestion_timestamp': datetime.now()
}

print("Bronze Layer Metadata:")
for key, value in metadata_summary.items():
    print(f"  {key}: {value}")

print("\n=== KEY TAKEAWAYS ===")
print("✓ Date partitioning enables efficient query pruning")
print("✓ Hierarchical structure separates structured/unstructured data")
print("✓ Metadata tracking enables data lineage and auditing")
print("✓ Bronze layer organization critical for downstream efficiency")
