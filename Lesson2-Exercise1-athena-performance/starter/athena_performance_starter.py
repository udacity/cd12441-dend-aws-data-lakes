"""
Lesson 2 - Exercise 1: Athena Query Performance Analysis (Starter)

This exercise demonstrates query performance differences between:
1. Structured data (no partitions) - Lesson 1 Exercise 1
2. Organized data (with partitions) - Lesson 1 Exercise 3

Students will create Athena tables and run benchmark queries.
"""

import boto3
import time
import os
import uuid
from datetime import datetime

BUCKET_NAME = os.environ.get('BUCKET_NAME', f'lakehouse-student-athena-{uuid.uuid4()}')
DATABASE_NAME = 'lakehouse_lesson2'
ATHENA_OUTPUT = f's3://{BUCKET_NAME}/athena-results/'

athena = boto3.client('athena', region_name='us-east-1')

def execute_query(query, wait=True):
    """Execute Athena query and optionally wait for completion"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    query_id = response['QueryExecutionId']
    
    if wait:
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_id)
            state = result['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return result
            time.sleep(0.5)
    return None

def create_database():
    print("\n[Step 1] Creating Athena Database...")
    # TODO: Execute CREATE DATABASE query
    print(f"✓ Database '{DATABASE_NAME}' created")

def create_structured_table():
    print("\n[Step 2] Creating Table for Structured Data...")
    print(f"  Location: s3://{BUCKET_NAME}/bronze/orders/")
    
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.orders_structured (
        order_id STRING,
        user_id STRING,
        product_id STRING,
        order_value DOUBLE,
        order_date DATE,
        status STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET_NAME}/bronze/orders/'
    """
    # TODO: Execute query
    print("✓ Table 'orders_structured' created")

def create_organized_table():
    print("\n[Step 3] Creating Table for Organized Data...")
    print(f"  Location: s3://{BUCKET_NAME}/bronze/structured/orders/raw/")
    
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.orders_organized (
        order_id STRING,
        user_id STRING,
        product_id STRING,
        order_value DOUBLE,
        order_date DATE,
        status STRING
    )
    PARTITIONED BY (date STRING)
    STORED AS PARQUET
    LOCATION 's3://{BUCKET_NAME}/bronze/structured/orders/raw/'
    """
    # TODO: Execute query
    # TODO: Run MSCK REPAIR TABLE to load partitions
    print("✓ Table 'orders_organized' created with partitions")

def benchmark_query(query, description):
    print(f"\n  {description}")
    start = time.time()
    # TODO: Execute query and get result
    result = # YOUR CODE HERE
    elapsed = time.time() - start
    
    # TODO: Get statistics from result
    stats = result['QueryExecution']['Statistics']
    data_scanned = stats.get('DataScannedInBytes', 0) / (1024 * 1024)
    
    print(f"     Execution Time: {elapsed:.2f}s")
    print(f"     Data Scanned: {data_scanned:.3f} MB")
    return elapsed, data_scanned

def run_benchmarks():
    print("\n[Step 4] Running Performance Benchmarks...")
    
    # TODO: Benchmark 1 - Full Table Scan
    print("\n  📊 Benchmark 1: Full Table Scan")
    # Run same query on both tables and compare
    
    # TODO: Benchmark 2 - Single Date Filter
    print("\n  📊 Benchmark 2: Single Date Filter")
    # Run date-filtered query on both tables and compare
    
    # TODO: Benchmark 3 - Date Range Filter
    print("\n  📊 Benchmark 3: Date Range Filter")
    # Run date-range query on both tables and compare

def print_summary():
    print("\n" + "="*70)
    print("EXERCISE 1 SUMMARY")
    print("="*70)
    print("\n✅ Key Findings:")
    print("  1. Partition pruning: 9-10x speedup for date-filtered queries")
    print("  2. Data scanned: 99% reduction with partitioning")
    print("  3. Cost savings: 99% for filtered queries ($5/TB)")
    print("  4. Full table scans: No benefit from partitioning")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 1: ATHENA QUERY PERFORMANCE ANALYSIS")
    print("="*70)
    
    create_database()
    create_structured_table()
    create_organized_table()
    run_benchmarks()
    print_summary()
