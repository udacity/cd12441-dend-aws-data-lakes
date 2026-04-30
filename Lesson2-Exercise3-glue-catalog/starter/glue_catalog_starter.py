"""
Lesson 2 - Exercise 3: AWS Glue Crawler and Data Catalog (Starter)

Create Glue crawler to automatically discover schemas and partitions.
Uses boto3 (no Spark required).
"""

import boto3
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
import io
from load_env import load_env

load_env()

BUCKET_NAME = os.environ.get('BUCKET_NAME')
DATABASE_NAME = 'swiftshop_catalog'
CRAWLER_NAME = 'swiftshop-orders-crawler'
ACCOUNT_ID = sts.get_caller_identity()['Account']
IAM_ROLE = os.environ.get('GLUE_ROLE_ARN', f'arn:aws:iam::{ACCOUNT_ID}:role/ecommerce-analytics-glue-role-dev')

glue = boto3.client('glue', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

def create_glue_database():
    print("\n[Step 1] Creating Glue Database...")
    
    # TODO: Create database using glue.create_database()
    # TODO: Handle AlreadyExistsException
    
    pass

def upload_sample_data():
    print("\n[Step 2] Preparing Sample Data...")
    
    np.random.seed(42)
    
    for i in range(3):
        date = (datetime.now() - timedelta(days=2-i)).strftime('%Y-%m-%d')
        
        # TODO: Generate 50 sample orders per partition
        orders = []
        for j in range(50):
            # YOUR CODE HERE - create order dict with order_id, user_id, product_id, order_value, status, created_at
            pass
        
        # TODO: Convert to DataFrame and upload as Parquet to S3
        # Key format: f"swiftshop/orders/order_date={date}/data.parquet"
        # YOUR CODE HERE
    
    print(f"  Total: 150 orders in 3 partitions")

def create_crawler():
    print("\n[Step 3] Creating Glue Crawler...")
    
    # TODO: Create crawler using glue.create_crawler()
    # Set S3 target to s3://{BUCKET_NAME}/swiftshop/orders/
    # Configure SchemaChangePolicy with UpdateBehavior and DeleteBehavior
    # Handle AlreadyExistsException
    
    pass

def run_crawler():
    print("\n[Step 4] Running Crawler...")
    
    # TODO: Start crawler using glue.start_crawler()
    # TODO: Poll crawler status until READY
    # TODO: Check LastCrawl status for SUCCEEDED/FAILED
    # TODO: Print metrics (TablesCreated, PartitionsCreated)
    
    pass

def examine_table():
    print("\n[Step 5] Examining Cataloged Table...")
    
    # TODO: Get table metadata using glue.get_table()
    # TODO: Display schema (columns and types)
    # TODO: Show partition keys
    # TODO: Display storage details (location, format)
    # TODO: List partitions using glue.get_partitions()
    
    pass

def demonstrate_schema_evolution():
    print("\n[Step 6] Demonstrating Schema Evolution...")
    
    # TODO: Upload new partition with extra column (shipping_address)
    # TODO: Print instructions to re-run crawler
    
    pass

def print_summary():
    print("\n" + "="*70)
    print("EXERCISE 3 SUMMARY")
    print("="*70)
    print("\n✅ Key Learnings:")
    print("   1. Glue Crawler automates schema discovery")
    print("   2. No manual DDL - reads Parquet metadata")
    print("   3. Automatic partition discovery from folders")
    print("   4. Schema evolution handled automatically")
    print("   5. Single catalog → Multiple query engines")
    print("\n🎯 Real-World Benefits:")
    print("   • 'Set it and forget it' automation")
    print("   • Consistent metadata across tools")
    print("   • Reduced operational overhead")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 3: AWS GLUE CRAWLER")
    print("="*70)
    
    if not BUCKET_NAME:
        print("\n❌ Error: BUCKET_NAME environment variable not set")
        print("   Run: export BUCKET_NAME='your-bucket-name'")
        exit(1)
    
    if not IAM_ROLE or IAM_ROLE == 'arn:aws:iam::123456789012:role/GlueServiceRole':
        print("\n⚠️  Warning: Using default IAM role ARN")
        print("   Set GLUE_ROLE_ARN environment variable with your Glue service role")
        print("   Example: export GLUE_ROLE_ARN='arn:aws:iam::YOUR_ACCOUNT:role/GlueServiceRole'")
        print("\n   Continuing with default (may fail)...\n")
    
    create_glue_database()
    upload_sample_data()
    create_crawler()
    run_crawler()
    examine_table()
    demonstrate_schema_evolution()
    print_summary()
