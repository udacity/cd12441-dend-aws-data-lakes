"""
Lesson 2 - Exercise 1: Athena Query Performance Analysis (Starter)

This exercise demonstrates query performance differences between:
1. Structured data (no partitions) - Lesson 1 Exercise 1
2. Unstructured data (no partitions) - Lesson 1 Exercise 2  
3. Organized data (with partitions) - Lesson 1 Exercise 3

Students will create Athena tables and run benchmark queries.
"""

import boto3
import time
import os
from datetime import datetime

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME')
DATABASE_NAME = 'lakehouse_lesson2'
ATHENA_OUTPUT = f's3://{BUCKET_NAME}/athena-results/'

# Initialize AWS clients
athena = boto3.client('athena', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

def create_database():
    """Create Athena database"""
    print("\n[Step 1] Creating Athena Database...")
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"
    # TODO: Execute query using athena.start_query_execution()
    # TODO: Wait for query completion
    print(f"✓ Database '{DATABASE_NAME}' created")

def create_structured_table():
    """Create table for structured data (Lesson 1 - Exercise 1)"""
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
    """Create table for organized data with partitions (Lesson 1 - Exercise 3)"""
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
    print("✓ Table 'orders_organized' created")

def benchmark_query(query, description):
    """Execute query and measure performance"""
    print(f"\n  Query: {description}")
    start_time = time.time()
    
    # TODO: Execute query using athena.start_query_execution()
    # TODO: Wait for completion
    # TODO: Get query statistics (execution time, data scanned)
    
    execution_time = time.time() - start_time
    data_scanned_mb = 0  # TODO: Get from query execution stats
    
    print(f"     Execution Time: {execution_time:.2f}s")
    print(f"     Data Scanned: {data_scanned_mb:.3f} MB")
    
    return execution_time, data_scanned_mb

def run_benchmarks():
    """Run performance benchmark queries"""
    print("\n[Step 4] Running Performance Benchmarks...")
    
    # Benchmark 1: Full table scan
    print("\n  📊 Benchmark 1: Full Table Scan")
    query1 = f"SELECT COUNT(*) FROM {DATABASE_NAME}.orders_structured WHERE order_value > 100"
    # TODO: Run query on both tables
    
    # Benchmark 2: Single date filter
    print("\n  📊 Benchmark 2: Single Date Filter")
    query2_structured = f"SELECT * FROM {DATABASE_NAME}.orders_structured WHERE order_date = DATE '2025-01-15'"
    query2_organized = f"SELECT * FROM {DATABASE_NAME}.orders_organized WHERE date = '2025-01-15'"
    # TODO: Run queries and compare
    
    # Benchmark 3: Date range filter
    print("\n  📊 Benchmark 3: Date Range Filter")
    query3_structured = f"SELECT * FROM {DATABASE_NAME}.orders_structured WHERE order_date BETWEEN DATE '2025-01-15' AND DATE '2025-01-20'"
    query3_organized = f"SELECT * FROM {DATABASE_NAME}.orders_organized WHERE date BETWEEN '2025-01-15' AND '2025-01-20'"
    # TODO: Run queries and compare

def print_summary():
    """Print performance summary"""
    print("\n" + "="*70)
    print("EXERCISE 1 SUMMARY")
    print("="*70)
    print("\n✅ Key Findings:")
    print("  1. Partition pruning provides 9-10x speedup for filtered queries")
    print("  2. Data scanned reduced by 99% with proper partitioning")
    print("  3. Cost savings of 99% for date-filtered queries")
    print("  4. No benefit for full table scans")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 1: ATHENA QUERY PERFORMANCE ANALYSIS")
    print("="*70)
    
    # TODO: Implement the exercise steps
    create_database()
    create_structured_table()
    create_organized_table()
    run_benchmarks()
    print_summary()
