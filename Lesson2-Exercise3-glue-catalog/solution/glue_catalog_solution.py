"""
Lesson 2 - Exercise 3: AWS Glue Crawler and Data Catalog (Solution)

Complete implementation using boto3.
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
IAM_ROLE = os.environ.get('GLUE_ROLE_ARN', 'arn:aws:iam::123456789012:role/GlueServiceRole')

glue = boto3.client('glue', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

def create_glue_database():
    print("\n[Step 1] Creating Glue Database...")
    
    try:
        glue.create_database(
            DatabaseInput={
                'Name': DATABASE_NAME,
                'Description': 'Metadata repository for SwiftShop data lake'
            }
        )
        print(f"  ✓ Database '{DATABASE_NAME}' created")
    except glue.exceptions.AlreadyExistsException:
        print(f"  ⚠️  Database '{DATABASE_NAME}' already exists")

def upload_sample_data():
    print("\n[Step 2] Preparing Sample Data...")
    
    np.random.seed(42)
    
    for i in range(3):
        date = (datetime.now() - timedelta(days=2-i)).strftime('%Y-%m-%d')
        
        # Generate orders
        orders = []
        for j in range(50):
            orders.append({
                'order_id': i * 50 + j,
                'user_id': f"user_{np.random.randint(1, 100):05d}",
                'product_id': f"prod_{np.random.randint(1, 50):03d}",
                'order_value': round(np.random.uniform(10, 500), 2),
                'status': np.random.choice(['pending', 'shipped', 'delivered']),
                'created_at': datetime.now()
            })
        
        df = pd.DataFrame(orders)
        
        # Upload to S3 with partition
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        key = f"swiftshop/orders/order_date={date}/data.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())
        
        print(f"  ✓ Uploaded partition: order_date={date} (50 orders)")
    
    print(f"  Total: 150 orders in 3 partitions")

def create_crawler():
    print("\n[Step 3] Creating Glue Crawler...")
    
    try:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=IAM_ROLE,
            DatabaseName=DATABASE_NAME,
            Targets={
                'S3Targets': [
                    {'Path': f's3://{BUCKET_NAME}/swiftshop/orders/'}
                ]
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            }
        )
        print(f"  ✓ Crawler '{CRAWLER_NAME}' created")
        print(f"  Target: s3://{BUCKET_NAME}/swiftshop/orders/")
    except glue.exceptions.AlreadyExistsException:
        print(f"  ⚠️  Crawler '{CRAWLER_NAME}' already exists")

def run_crawler():
    print("\n[Step 4] Running Crawler...")
    
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        print(f"  Starting crawler...")
    except glue.exceptions.CrawlerRunningException:
        print(f"  ⚠️  Crawler already running")
        return
    except Exception as e:
        print(f"  ✗ Failed to start crawler: {e}")
        return
    
    # Wait for completion
    max_wait = 300  # 5 minutes
    elapsed = 0
    while elapsed < max_wait:
        response = glue.get_crawler(Name=CRAWLER_NAME)
        state = response['Crawler']['State']
        
        if state == 'READY':
            last_crawl = response['Crawler'].get('LastCrawl', {})
            status = last_crawl.get('Status', 'UNKNOWN')
            
            if status == 'SUCCEEDED':
                print(f"  ✓ Crawler completed successfully")
                
                metrics = last_crawl.get('Metrics', {})
                print(f"  Tables created: {metrics.get('TablesCreated', 0)}")
                print(f"  Partitions created: {metrics.get('PartitionsCreated', 0)}")
                break
            elif status == 'FAILED':
                error_msg = last_crawl.get('ErrorMessage', 'Unknown error')
                print(f"  ✗ Crawler failed: {error_msg}")
                break
        
        time.sleep(5)
        elapsed += 5
    
    if elapsed >= max_wait:
        print(f"  ⚠️  Crawler timeout after {max_wait}s")

def examine_table():
    print("\n[Step 5] Examining Cataloged Table...")
    
    try:
        response = glue.get_table(DatabaseName=DATABASE_NAME, Name='orders')
        table = response['Table']
        
        print(f"\n  📊 Table: {table['Name']}")
        print(f"\n  Schema:")
        for col in table['StorageDescriptor']['Columns']:
            print(f"     {col['Name']:<20} {col['Type']}")
        
        print(f"\n  Partition Keys:")
        for key in table['PartitionKeys']:
            print(f"     {key['Name']} ({key['Type']})")
        
        print(f"\n  Storage:")
        print(f"     Location: {table['StorageDescriptor']['Location']}")
        print(f"     Format: {table['StorageDescriptor']['SerdeInfo']['SerializationLibrary']}")
        
        # List partitions
        partitions = glue.get_partitions(DatabaseName=DATABASE_NAME, TableName='orders')
        print(f"\n  Partitions: {len(partitions['Partitions'])}")
        for p in partitions['Partitions']:
            print(f"     {p['Values'][0]}")
    except glue.exceptions.EntityNotFoundException:
        print(f"\n  ⚠️  Table 'orders' not found")
        print(f"  Crawler may not have completed successfully")
        print(f"  Check crawler status in AWS Glue console")

def demonstrate_schema_evolution():
    print("\n[Step 6] Demonstrating Schema Evolution...")
    
    print("  Adding new partition with extra column...")
    
    date = datetime.now().strftime('%Y-%m-%d')
    orders = []
    for i in range(50):
        orders.append({
            'order_id': 200 + i,
            'user_id': f"user_{np.random.randint(1, 100):05d}",
            'product_id': f"prod_{np.random.randint(1, 50):03d}",
            'order_value': round(np.random.uniform(10, 500), 2),
            'status': np.random.choice(['pending', 'shipped', 'delivered']),
            'created_at': datetime.now(),
            'shipping_address': f"{np.random.randint(1, 999)} Main St"  # NEW COLUMN
        })
    
    df = pd.DataFrame(orders)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    key = f"swiftshop/orders/order_date={date}/data.parquet"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())
    
    print(f"  ✓ New partition uploaded with 'shipping_address' column")
    print(f"  Run crawler again to detect schema change")

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
    
    if not IAM_ROLE or IAM_ROLE == 'arn:aws:iam::123456789012:role/ecommerce-analytics-glue-role-dev':
        print("\n⚠️  Warning: Using default IAM role ARN")
        print("   Set GLUE_ROLE_ARN environment variable with your Glue service role")
        print("   Example: export GLUE_ROLE_ARN='arn:aws:iam::YOUR_ACCOUNT:role/ecommerce-analytics-glue-role-dev'")
        print("\n   Continuing with default (may fail)...\n")
    
    create_glue_database()
    upload_sample_data()
    create_crawler()
    run_crawler()
    examine_table()
    demonstrate_schema_evolution()
    print_summary()
