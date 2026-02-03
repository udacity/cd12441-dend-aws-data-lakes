# Exercise 3: Partitioning and Optimization - Student Instructions

## Objective
Optimize CDC pipeline with date-based partitioning and file size optimization for efficient querying and storage.

## What You'll Learn
- Implement Hive-style date partitioning
- Optimize file sizes with coalesce
- Understand partition pruning benefits
- Query partitioned data efficiently

## Prerequisites
- Exercise 2 completed (CDC with bookmarks working)
- Pre-configured AWS Glue environment
- Understanding of partitioning concepts
- Basic knowledge of query optimization

## Step-by-Step Instructions

### Step 1: Add Partition Column
Extract date from `updated_at` timestamp:
```python
df = df.withColumn("order_date", col("updated_at").cast("date"))
```
This creates a new column for partitioning.

### Step 2: Optimize File Sizes with Coalesce
Before writing, reduce the number of partitions:
```python
df.coalesce(4)
```
This prevents creating too many small files (small file problem).

### Step 3: Write with Partitioning
Use `partitionBy` to create Hive-style partitions:
```python
.partitionBy("order_date")
```

### Step 4: Complete the Write Chain
```python
df.coalesce(4) \
  .write \
  .mode("append") \
  .partitionBy("order_date") \
  .parquet("s3://cloudmart/bronze/orders/")
```

## Understanding the S3 Structure
After partitioning, your S3 structure will look like:
```
s3://cloudmart/bronze/orders/
├── order_date=2024-01-15/
│   ├── part-00000.parquet
│   └── part-00001.parquet
├── order_date=2024-01-16/
│   └── part-00000.parquet
└── order_date=2024-01-17/
    └── part-00000.parquet
```

## Testing Your Implementation

### Run the Job
```bash
aws glue start-job-run \
  --job-name partitioning-optimization-exercise \
  --arguments '--bookmark=2024-01-01 00:00:00'
```

### Verify Partitions in S3
```bash
aws s3 ls s3://cloudmart/bronze/orders/ --recursive
```

### Create Partitioned Table in Athena
```sql
CREATE EXTERNAL TABLE bronze_orders_partitioned (
  order_id bigint,
  user_id string,
  order_value double,
  updated_at timestamp
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://cloudmart/bronze/orders/';

-- Discover partitions
MSCK REPAIR TABLE bronze_orders_partitioned;
```

## Performance Comparison

### Without Partition Pruning (slow)
```sql
SELECT COUNT(*) FROM bronze_orders_partitioned;
-- Scans all files
```

### With Partition Pruning (fast)
```sql
SELECT COUNT(*) FROM bronze_orders_partitioned 
WHERE order_date = '2024-01-15';
-- Only scans files in order_date=2024-01-15/ partition
```

## Expected Output
```
Starting optimized CDC with partitioning
Records retrieved: 150
Records processed: 150
Partitioned by order_date
Next bookmark: 2024-01-15 18:45:22
```

## Common Issues
- **Too many small files**: Increase coalesce number
- **Partitions not showing**: Run `MSCK REPAIR TABLE`
- **Slow queries**: Ensure WHERE clause includes partition column

## Success Criteria
✅ Data partitioned by order_date in S3  
✅ Optimized file sizes (not thousands of tiny files)  
✅ Queries with partition filter are faster  
✅ Athena shows partition structure

## Bonus Challenge
Compare query performance:
1. Query without partition filter (full scan)
2. Query with partition filter (partition pruning)
3. Check "Data scanned" in Athena query results
