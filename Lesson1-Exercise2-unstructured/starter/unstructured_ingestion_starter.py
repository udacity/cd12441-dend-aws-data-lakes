"""
Exercise 2: Bronze Layer - Unstructured Data Ingestion
Student Name: _______________
Date: _______________

Learning Objectives:
- Load unstructured JSON data from S3
- Understand schema-on-read approach
- Handle nested JSON structures
- Save to bronze layer preserving flexibility

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run the script and observe schema inference
3. Compare with Exercise 1 structured approach
"""

import pandas as pd
import json
import boto3
from datetime import datetime
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')
source_bucket = 'swiftshop-data'
bronze_bucket = 'swiftshop-lakehouse'

print("=== EXERCISE 2: BRONZE LAYER - UNSTRUCTURED DATA ===\n")

# Step 1: Load Unstructured Data
print("STEP 1: LOAD UNSTRUCTURED DATA (JSON)")
print("-" * 50)

# TODO: Load JSON lines file from S3
json_obj = s3.get_object(Bucket=source_bucket, Key='raw/clickstream.json')
# TODO: Parse each line as JSON
raw_logs = # YOUR CODE HERE (Hint: split by '\n' and use json.loads())

print("Unstructured Data Schema (Schema-on-Read):")
# TODO: Examine the first log entry
print(f"Sample record keys: {# YOUR CODE HERE}")
print(f"Sample record:\n{json.dumps(raw_logs[0], indent=2)}\n")

# Step 2: Schema-on-Read - Flatten Nested JSON
print("STEP 2: FLATTEN NESTED JSON (SCHEMA-ON-READ)")
print("-" * 50)

# TODO: Flatten the nested JSON structure
flattened_data = []
for log in raw_logs:
    # TODO: Extract fields from nested structure
    user_id = # YOUR CODE HERE
    timestamp = # YOUR CODE HERE
    event_type = # YOUR CODE HERE
    page_url = # YOUR CODE HERE
    
    # TODO: Extract nested metadata
    metadata = log.get('metadata', {})
    browser = # YOUR CODE HERE
    device = # YOUR CODE HERE
    
    # TODO: Handle optional product_id field
    product_id = # YOUR CODE HERE (Hint: use .get() with default None)
    
    flattened_data.append({
        'user_id': user_id,
        'timestamp': timestamp,
        'event_type': event_type,
        'page_url': page_url,
        'browser': browser,
        'device': device,
        'product_id': product_id
    })

clickstream_df = pd.DataFrame(flattened_data)

print("Flattened Schema:")
print(f"Columns: {list(clickstream_df.columns)}")
print(f"Data types:\n{clickstream_df.dtypes}")
print(f"Shape: {clickstream_df.shape}")
print(f"Sample flattened data:\n{clickstream_df.head(3)}\n")

# Step 3: Add Metadata Columns
print("STEP 3: ADD METADATA FOR TRACKING")
print("-" * 50)

# TODO: Add metadata columns
clickstream_df['ingestion_timestamp'] = # YOUR CODE HERE
clickstream_df['source_system'] = # YOUR CODE HERE
clickstream_df['schema_version'] = # YOUR CODE HERE

print("Metadata columns added:")
print(f"New columns: {list(clickstream_df.columns[-3:])}\n")

# Step 4: Save to Bronze Layer
print("STEP 4: SAVE TO BRONZE LAYER")
print("-" * 50)

# TODO: Save to S3 bronze layer as Parquet
bronze_key = 'bronze/unstructured/clickstream/clickstream.parquet'

parquet_buffer = BytesIO()
# YOUR CODE HERE - save DataFrame to parquet_buffer

# YOUR CODE HERE - upload to S3

print(f"✓ Data saved to s3://{bronze_bucket}/{bronze_key}")
print(f"✓ Records saved: {len(clickstream_df)}")
print(f"✓ Bronze layer ingestion complete!\n")

# Step 5: Compare with Structured Data
print("STEP 5: SCHEMA FLEXIBILITY COMPARISON")
print("-" * 50)

# TODO: Calculate statistics about variable fields
total_records = len(clickstream_df)
records_with_product = # YOUR CODE HERE (Hint: count non-null product_id)
percentage_with_product = # YOUR CODE HERE

print(f"Total clickstream events: {total_records}")
print(f"Events with product_id: {records_with_product} ({percentage_with_product:.1f}%)")
print(f"Events without product_id: {total_records - records_with_product}")

print("\n=== KEY TAKEAWAYS ===")
print("✓ Unstructured data has flexible schema (schema-on-read)")
print("✓ JSON handles nested structures and variable fields")
print("✓ Schema inferred when reading, not when writing")
print("✓ Bronze layer preserves flexibility for future analysis")
