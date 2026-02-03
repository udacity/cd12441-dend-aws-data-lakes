# Exercise 3: Bronze Layer Organization - Student Instructions

## Objective
Organize bronze layer with hierarchical structure and date partitioning for efficient querying.

## What You'll Learn
- Design hierarchical bronze layer structure
- Implement date-based partitioning
- Separate structured vs unstructured data
- Enable efficient incremental queries
- Track metadata for lineage

## Prerequisites
- Exercise 1 & 2 completed (data in bronze)
- Pre-configured Docker container with all dependencies
- Understanding of partitioning concepts
- Knowledge of Hive-style partitioning

## Step-by-Step Instructions

### Step 1: Load Data from Bronze
Load previously saved data:
```python
orders_obj = s3.get_object(Bucket=bronze_bucket, Key='bronze/structured/orders/orders.parquet')
orders_df = pd.read_parquet(BytesIO(orders_obj['Body'].read()))

clickstream_obj = s3.get_object(Bucket=bronze_bucket, Key='bronze/unstructured/clickstream/clickstream.parquet')
clickstream_df = pd.read_parquet(BytesIO(clickstream_obj['Body'].read()))
```

### Step 2: Add Partition Columns
Extract date for partitioning:
```python
# For orders
orders_df['partition_date'] = pd.to_datetime(orders_df['order_date']).dt.date

# For clickstream
clickstream_df['partition_date'] = pd.to_datetime(clickstream_df['timestamp']).dt.date
```

### Step 3: Save with Date Partitioning
Write each date partition separately:
```python
# Partition orders by date
for date, group in orders_df.groupby('partition_date'):
    partition_key = f'bronze/structured/orders/date={date}/orders.parquet'
    
    parquet_buffer = BytesIO()
    group.to_parquet(parquet_buffer, index=False)
    
    s3.put_object(
        Bucket=bronze_bucket,
        Key=partition_key,
        Body=parquet_buffer.getvalue()
    )
```

### Step 4: Repeat for Clickstream
Apply same pattern to clickstream data:
```python
for date, group in clickstream_df.groupby('partition_date'):
    partition_key = f'bronze/unstructured/clickstream/date={date}/clickstream.parquet'
    # ... save partition
```

## Understanding Bronze Layer Organization

### Hierarchical Structure
```
bronze/
├── structured/           # Fixed schema data
│   └── orders/
│       ├── date=2024-01-15/
│       │   └── orders.parquet
│       └── date=2024-01-16/
│           └── orders.parquet
└── unstructured/         # Flexible schema data
    └── clickstream/
        ├── date=2024-01-15/
        │   └── clickstream.parquet
        └── date=2024-01-16/
            └── clickstream.parquet
```

### Benefits of Partitioning
- **Query efficiency**: Only scan relevant partitions
- **Incremental loads**: Easy to identify new data
- **Cost savings**: Reduced data scanned in Athena
- **Organization**: Clear data structure

## Expected Output
```
=== EXERCISE 3: BRONZE LAYER ORGANIZATION ===

STEP 1: LOAD DATA FROM BRONZE LAYER
Orders loaded: 1000 records
Clickstream loaded: 5000 records

STEP 2: ADD DATE PARTITION COLUMNS
Partition columns added:
Orders date range: 2024-01-15 to 2024-01-20
Clickstream date range: 2024-01-15 to 2024-01-20
Unique dates in orders: 6
Unique dates in clickstream: 6

STEP 3: SAVE WITH DATE PARTITIONING
✓ Orders saved to 6 date partitions
✓ Clickstream saved to 6 date partitions

STEP 4: VERIFY ORGANIZATION
Bronze layer structure:
bronze/structured/orders/date=2024-01-15/
bronze/structured/orders/date=2024-01-16/
bronze/unstructured/clickstream/date=2024-01-15/
bronze/unstructured/clickstream/date=2024-01-16/
```

## Verification
List partitions in S3:
```bash
aws s3 ls s3://swiftshop-lakehouse/bronze/structured/orders/ --recursive
aws s3 ls s3://swiftshop-lakehouse/bronze/unstructured/clickstream/ --recursive
```

Query with Athena (partition pruning):
```sql
CREATE EXTERNAL TABLE bronze_orders (
  order_id bigint,
  user_id string,
  order_value double
)
PARTITIONED BY (date string)
STORED AS PARQUET
LOCATION 's3://swiftshop-lakehouse/bronze/structured/orders/';

MSCK REPAIR TABLE bronze_orders;

-- Efficient query with partition filter
SELECT COUNT(*) FROM bronze_orders WHERE date = '2024-01-15';
```

## Common Issues
- **Date parsing errors**: Ensure correct datetime format
- **Empty partitions**: Check groupby logic
- **S3 path errors**: Verify Hive-style format `key=value`

## Success Criteria
✅ Data partitioned by date  
✅ Hierarchical structure created  
✅ Structured/unstructured separated  
✅ Partitions queryable in Athena  
✅ Partition pruning working
