"""
Exercise 1: Bronze Layer - Structured Data Ingestion
Student Name: _______________
Date: _______________

Learning Objectives:
- Load structured Parquet data from S3
- Understand schema-on-write approach
- Add metadata columns for tracking
- Save to bronze layer with proper organization

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run the script and verify outputs
3. Check S3 bronze layer for saved data
"""

import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')
source_bucket = 'swiftshop-data'
bronze_bucket = 'swiftshop-lakehouse'

print("=== EXERCISE 1: BRONZE LAYER - STRUCTURED DATA ===\n")

# Step 1: Load Structured Data
print("STEP 1: LOAD STRUCTURED DATA (PARQUET)")
print("-" * 50)

# TODO: Load the Parquet file from S3
# Hint: Use s3.get_object() and pd.read_parquet()
parquet_obj = s3.get_object(Bucket=source_bucket, Key='raw/orders.parquet')
orders_df = # YOUR CODE HERE

print("Structured Data Schema (Schema-on-Write):")
# TODO: Print column names, data types, and shape
print(f"Columns: {# YOUR CODE HERE}")
print(f"Data types:\n{# YOUR CODE HERE}")
print(f"Shape: {# YOUR CODE HERE}")
print(f"Sample data:\n{orders_df.head(3)}\n")

# Step 2: Add Metadata Columns
print("STEP 2: ADD METADATA FOR TRACKING")
print("-" * 50)

# TODO: Add metadata columns
# - ingestion_timestamp: current datetime
# - source_system: 'ecommerce_db'
# - schema_version: '1.0'
orders_df['ingestion_timestamp'] = # YOUR CODE HERE
orders_df['source_system'] = # YOUR CODE HERE
orders_df['schema_version'] = # YOUR CODE HERE

print("Metadata columns added:")
print(f"New columns: {list(orders_df.columns[-3:])}")
print(f"Sample with metadata:\n{orders_df[['order_id', 'ingestion_timestamp', 'source_system']].head(3)}\n")

# Step 3: Save to Bronze Layer
print("STEP 3: SAVE TO BRONZE LAYER")
print("-" * 50)

# TODO: Save to S3 bronze layer as Parquet
# Path: s3://swiftshop-lakehouse/bronze/structured/orders/
bronze_key = 'bronze/structured/orders/orders.parquet'

# Convert to Parquet bytes
parquet_buffer = BytesIO()
# YOUR CODE HERE - save DataFrame to parquet_buffer

# Upload to S3
# YOUR CODE HERE - use s3.put_object()

print(f"✓ Data saved to s3://{bronze_bucket}/{bronze_key}")
print(f"✓ Records saved: {len(orders_df)}")
print(f"✓ Bronze layer ingestion complete!\n")

# Step 4: Verification
print("STEP 4: VERIFY BRONZE LAYER")
print("-" * 50)

# TODO: Read back from bronze layer to verify
verify_obj = # YOUR CODE HERE
verify_df = # YOUR CODE HERE

print(f"Verification:")
print(f"Records in bronze: {len(verify_df)}")
print(f"Metadata columns present: {all(col in verify_df.columns for col in ['ingestion_timestamp', 'source_system', 'schema_version'])}")
print(f"Schema matches: {list(orders_df.columns) == list(verify_df.columns)}")

print("\n=== KEY TAKEAWAYS ===")
print("✓ Structured data has fixed schema (schema-on-write)")
print("✓ Bronze layer preserves raw data with metadata")
print("✓ Parquet format is efficient for structured data")
print("✓ Metadata enables data lineage tracking")
