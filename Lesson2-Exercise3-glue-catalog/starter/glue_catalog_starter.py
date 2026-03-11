"""
Lesson 2 - Exercise 3: AWS Glue Crawler and Data Catalog (Starter)

Create Glue crawler to automatically discover schemas and partitions.
Uses boto3 (no Spark required).
"""

import boto3
import pandas as pd
import time
import os
from datetime import datetime, timedelta

BUCKET_NAME = os.environ.get('BUCKET_NAME')
DATABASE_NAME = 'swiftshop_catalog'
CRAWLER_NAME = 'swiftshop-orders-crawler'

glue = boto3.client('glue', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
athena = boto3.client('athena', region_name='us-east-1')

def create_glue_database():
    """Create Glue database"""
    print("\n[Step 1] Creating Glue Database...")
    
    # TODO: Create database using glue.create_database()
    # TODO: Handle case where database already exists
    
    pass

def upload_sample_data():
    """Upload partitioned sample data to S3"""
    print("\n[Step 2] Preparing Sample Data...")
    
    # TODO: Generate sample orders data
    # TODO: Create 3 date partitions
    # TODO: Upload as Parquet files to S3
    
    pass

def create_crawler():
    """Create Glue crawler"""
    print("\n[Step 3] Creating Glue Crawler...")
    
    # TODO: Define crawler configuration
    # TODO: Set S3 target path
    # TODO: Configure schema inference and partition discovery
    # TODO: Create crawler using glue.create_crawler()
    
    pass

def run_crawler():
    """Start crawler and wait for completion"""
    print("\n[Step 4] Running Crawler...")
    
    # TODO: Start crawler using glue.start_crawler()
    # TODO: Poll crawler status until SUCCEEDED or FAILED
    # TODO: Print progress updates
    
    pass

def examine_table():
    """Examine cataloged table metadata"""
    print("\n[Step 5] Examining Cataloged Table...")
    
    # TODO: Get table metadata using glue.get_table()
    # TODO: Display schema (columns and types)
    # TODO: Show partition information
    # TODO: Display storage details
    
    pass

def query_with_athena():
    """Query cataloged data using Athena"""
    print("\n[Step 6] Querying with Athena...")
    
    # TODO: Execute COUNT(*) query
    # TODO: Execute partition-filtered query
    # TODO: Compare execution times and data scanned
    
    pass

def demonstrate_schema_evolution():
    """Show how crawler handles schema changes"""
    print("\n[Step 7] Demonstrating Schema Evolution...")
    
    # TODO: Upload new partition with additional column
    # TODO: Run crawler again
    # TODO: Show updated schema
    
    pass

def show_integration_options():
    """Display how cataloged data can be queried"""
    print("\n[Step 8] Glue Catalog Integration...")
    
    print("\n  The cataloged table is queryable by:")
    print("  ✓ Amazon Athena")
    print("  ✓ Amazon EMR (Spark)")
    print("  ✓ Amazon Redshift Spectrum")
    print("  ✓ AWS Glue ETL Jobs")

def print_summary():
    """Print exercise summary"""
    print("\n" + "="*70)
    print("EXERCISE 3 SUMMARY")
    print("="*70)
    print("\n✅ Key Learnings:")
    print("   1. Glue Crawler automates schema discovery")
    print("   2. No manual DDL - reads Parquet metadata")
    print("   3. Automatic partition discovery")
    print("   4. Schema evolution handled automatically")
    print("   5. Single catalog → Multiple query engines")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 3: AWS GLUE CRAWLER")
    print("="*70)
    
    # TODO: Implement exercise steps
    create_glue_database()
    upload_sample_data()
    create_crawler()
    run_crawler()
    examine_table()
    query_with_athena()
    demonstrate_schema_evolution()
    show_integration_options()
    print_summary()
