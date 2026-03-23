# Exercise 1: Structured Data Ingestion - Student Instructions

## Objective
Load structured Parquet data into the bronze layer, understanding schema-on-write principles.

## What You'll Learn
- Load Parquet files using pandas
- Understand schema-on-write (schema enforced at write time)
- Assess data quality in raw bronze data
- Create S3 buckets and upload data
- Verify data integrity after S3 write

## Prerequisites
- Pre-configured Docker container with all dependencies installed
- AWS credentials already configured in environment
- Sample data: `data/orders.parquet` available locally
- All Python packages (boto3, pandas, pyarrow, python-dotenv) pre-installed

## Step-by-Step Instructions

### Step 1: Initialize S3 Client
```python
s3_client = boto3.client('s3')
```

### Step 2: Load Parquet Data
```python
orders_df = pd.read_parquet(LOCAL_DATA_PATH)
```

### Step 3: Examine Schema
Print the DataFrame's data types:
```python
orders_df.dtypes.to_string()
```

### Step 4: Preview Data
```python
orders_df.head(5).to_string()
```

### Step 5: Data Quality Assessment
Count nulls and duplicates:
```python
null_values = orders_df['order_value'].isna().sum()
duplicates = orders_df.duplicated(subset=['order_id']).sum()
```

### Step 6: Create S3 Bucket
List existing buckets and create if needed:
```python
buckets = s3_client.list_buckets(
    MaxBuckets=1,
    Prefix="lakehouse-student-bronze-",
    BucketRegion='us-east-1'
)['Buckets']

s3_client.create_bucket(ACL='private', Bucket=BUCKET_NAME)
```

### Step 7: Write to S3
Write DataFrame to buffer and upload:
```python
buffer = BytesIO()
orders_df.to_parquet(buffer, index=False, coerce_timestamps='ms', allow_truncated_timestamps=True)
buffer.seek(0)
s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH, Body=buffer.getvalue())
```

### Step 8: Verify S3 Write
Read back from S3 and compare row counts:
```python
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH)
bronze_df = pd.read_parquet(BytesIO(response['Body'].read()))
```

## Understanding Schema-on-Write
- **Schema defined at write time**: Parquet enforces column types
- **Type safety**: Cannot write wrong data types
- **Efficient storage**: Columnar format with compression
- **Fast queries**: Predicate pushdown and column pruning

## Expected Output
```
[Step 2] Loading structured data (orders.parquet)...
✓ Data loaded in 0.05 seconds
  Rows: 10,000
  Columns: 6

[Step 5] Data Quality Assessment (Raw Bronze Layer)...
  Total rows: 10,000
  Null order_value: 520 (5.2%)
  Duplicate order_ids: 210 (2.1%)

[Step 7] Writing to S3 Bronze Layer...
✓ Data written to S3

[Step 8] Verifying S3 Bronze Layer...
✓ Verification successful
  Match: ✓
```

## Common Issues
- **S3 access denied**: Check IAM permissions
- **Parquet read error**: Verify file format
- **BucketAlreadyExists**: Bucket names are globally unique

## Success Criteria
✅ Parquet file loaded successfully
✅ Schema displayed correctly
✅ Data quality issues identified (nulls, duplicates)
✅ S3 bucket created
✅ Data saved to bronze layer in S3
✅ Verification confirms row count match
