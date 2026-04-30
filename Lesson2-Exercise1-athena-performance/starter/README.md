# Lesson 2 - Exercise 1: Athena Query Performance Analysis (Starter)

## Objective
Create Athena tables for different data organizations and benchmark query performance to demonstrate the impact of proper data organization.

## Prerequisites

### Required Setup
**AWS credentials** in `/workspace/.env` (see the course **Setup** page; re-paste from the Cloud Resources tab if your session token has expired). This exercise also requires `BUCKET_NAME` to be set in `/workspace/.env` — use the same bucket name from Lesson 1.

**Complete Lesson 1 exercises first:**
- Exercise 1: Structured data ingestion → `s3://bucket/bronze/orders/`
- Exercise 3: Data organization → `s3://bucket/bronze/structured/orders/raw/`

### Verify Prerequisites
```bash
export BUCKET_NAME="your-bucket-name"
python setup.py
```

Expected output:
```
[Step 1] Verifying Lesson 1 Data...
  ✓ Lesson 1 - Exercise 1 (Structured)
  ✓ Lesson 1 - Exercise 3 (Organized)

[Step 2] Setting up Athena Output Location...
  ✓ s3://bucket/athena-results/

SETUP COMPLETE
```

## Instructions

### Step 1: Complete the Starter Code
Open `athena_performance_starter.py` and implement the TODO sections:

#### TODO 1: Execute Athena Query
```python
def execute_query(query, wait=True):
    """Execute Athena query and wait for completion"""
    # Start query execution
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    
    # Get query execution ID
    query_id = response['QueryExecutionId']
    
    # Wait for completion
    if wait:
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_id)
            state = result['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return result
            time.sleep(0.5)
```

#### TODO 2: Create Database
```python
def create_database():
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"
    execute_query(query)
```

#### TODO 3: Create Structured Table
```python
def create_structured_table():
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
    execute_query(query)
```

#### TODO 4: Create Organized Table with Partitions
```python
def create_organized_table():
    # Create table
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
    execute_query(query)
    
    # Load partitions
    execute_query(f"MSCK REPAIR TABLE {DATABASE_NAME}.orders_organized")
```

#### TODO 5: Benchmark Query Function
```python
def benchmark_query(query, description):
    print(f"\n  {description}")
    start_time = time.time()
    
    # Execute query
    result = execute_query(query)
    
    # Calculate metrics
    execution_time = time.time() - start_time
    stats = result['QueryExecution']['Statistics']
    data_scanned_mb = stats.get('DataScannedInBytes', 0) / (1024 * 1024)
    
    print(f"     Execution Time: {execution_time:.2f}s")
    print(f"     Data Scanned: {data_scanned_mb:.3f} MB")
    
    return execution_time, data_scanned_mb
```

#### TODO 6: Run Benchmarks
```python
def run_benchmarks():
    # Benchmark 1: Full table scan
    print("\n  📊 Benchmark 1: Full Table Scan")
    t1_s, d1_s = benchmark_query(
        f"SELECT COUNT(*) FROM {DATABASE_NAME}.orders_structured WHERE order_value > 100",
        "Structured (no partitions)"
    )
    t1_o, d1_o = benchmark_query(
        f"SELECT COUNT(*) FROM {DATABASE_NAME}.orders_organized WHERE order_value > 100",
        "Organized (with partitions)"
    )
    
    # Benchmark 2: Single date filter
    print("\n  📊 Benchmark 2: Single Date Filter")
    t2_s, d2_s = benchmark_query(
        f"SELECT * FROM {DATABASE_NAME}.orders_structured WHERE order_date = DATE '2025-01-15'",
        "Structured (no partitions)"
    )
    t2_o, d2_o = benchmark_query(
        f"SELECT * FROM {DATABASE_NAME}.orders_organized WHERE date = '2025-01-15'",
        "Organized (with partitions)"
    )
    
    # Calculate improvement
    speedup = t2_s / t2_o if t2_o > 0 else 0
    scan_reduction = (1 - d2_o / d2_s) * 100 if d2_s > 0 else 0
    print(f"  ✅ {speedup:.1f}x faster, {scan_reduction:.1f}% less data scanned")
```

### Step 2: Run the Exercise
```bash
export BUCKET_NAME="your-bucket-name"
python athena_performance_starter.py
```

### Step 3: Analyze Results
Compare the performance metrics:
- Execution time (seconds)
- Data scanned (MB)
- Cost implications ($5 per TB)

## Expected Results

### Full Table Scan
- **Structured**: ~2.3s, 1.2 MB scanned
- **Organized**: ~2.4s, 1.2 MB scanned
- **Improvement**: None (must scan all data)

### Single Date Filter
- **Structured**: ~2.2s, 1.2 MB scanned (entire dataset)
- **Organized**: ~0.2s, 0.003 MB scanned (single partition)
- **Improvement**: 9-10x faster, 99% less data

### Date Range Filter
- **Structured**: ~2.3s, 1.2 MB scanned (entire dataset)
- **Organized**: ~0.3s, 0.02 MB scanned (6 partitions)
- **Improvement**: 7-8x faster, 98% less data

## Key Learnings

1. **Partition Pruning**: Athena skips irrelevant partitions based on WHERE clause
2. **Cost Optimization**: 99% reduction in data scanned = 99% cost savings
3. **Query Patterns**: Benefit only applies to queries with partition filters
4. **Trade-offs**: No benefit for full table scans, slight overhead for partition metadata

## Verification

Check Athena query history:
```bash
aws athena list-query-executions --max-results 10
```

View query details:
```bash
aws athena get-query-execution --query-execution-id <execution-id>
```

## Common Issues

| Issue | Solution |
|-------|----------|
| `Database not found` | Verify DATABASE_NAME variable |
| `Table not found` | Check S3 locations from Lesson 1 |
| `Partitions not loaded` | Run `MSCK REPAIR TABLE` |
| `Access denied` | Configure Athena result location |

## Next Steps
After completing this exercise, you understand:
- How data organization impacts query performance
- The cost benefits of partitioning
- When to use partitions vs flat structures

Proceed to **Lesson 2 - Exercise 2** for CDC with bookmarks.
