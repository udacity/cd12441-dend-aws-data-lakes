# Exercise 1: Structured Data Ingestion - Student Instructions

## Objective
Load structured Parquet data from S3 into the bronze layer, understanding schema-on-write principles.

## What You'll Learn
- Load Parquet files from S3 using boto3 and pandas
- Understand schema-on-write (schema enforced at write time)
- Add metadata columns for data lineage tracking
- Save data to bronze layer with proper organization

## Prerequisites
- Pre-configured Docker container with all dependencies installed
- AWS credentials already configured in environment
- Sample data: `orders.parquet` available in source S3 bucket
- All Python packages (boto3, pandas, pyarrow) pre-installed

## Step-by-Step Instructions

### Step 1: Load Parquet from S3
Complete the data loading:
```python
parquet_obj = s3.get_object(Bucket=source_bucket, Key='raw/orders.parquet')
orders_df = pd.read_parquet(BytesIO(parquet_obj['Body'].read()))
```

### Step 2: Examine Schema
Print schema information:
```python
print(f"Columns: {orders_df.columns.tolist()}")
print(f"Data types:\n{orders_df.dtypes}")
print(f"Shape: {orders_df.shape}")
```

### Step 3: Add Metadata Columns
Add tracking columns:
```python
orders_df['ingestion_timestamp'] = datetime.now()
orders_df['source_system'] = 'swiftshop-orders'
orders_df['source_file'] = 'raw/orders.parquet'
```

### Step 4: Save to Bronze Layer
Write to S3 bronze layer:
```python
parquet_buffer = BytesIO()
orders_df.to_parquet(parquet_buffer, index=False)
s3.put_object(
    Bucket=bronze_bucket,
    Key='bronze/structured/orders/orders.parquet',
    Body=parquet_buffer.getvalue()
)
```

## Understanding Schema-on-Write
- **Schema defined at write time**: Parquet enforces column types
- **Type safety**: Cannot write wrong data types
- **Efficient storage**: Columnar format with compression
- **Fast queries**: Predicate pushdown and column pruning

## Expected Output
```
=== EXERCISE 1: BRONZE LAYER - STRUCTURED DATA ===

STEP 1: LOAD STRUCTURED DATA (PARQUET)
Columns: ['order_id', 'user_id', 'order_date', 'order_value', 'status']
Data types:
order_id         int64
user_id         object
order_date      object
order_value    float64
status          object
Shape: (1000, 5)

STEP 2: ADD METADATA FOR TRACKING
Metadata columns added: ingestion_timestamp, source_system, source_file

STEP 3: SAVE TO BRONZE LAYER
✓ Data saved to bronze/structured/orders/orders.parquet
```

## Verification
Check S3 for the file:
```bash
aws s3 ls s3://swiftshop-lakehouse/bronze/structured/orders/
```

## Common Issues
- **S3 access denied**: Check IAM permissions
- **Parquet read error**: Verify file format
- **Memory error**: File too large for pandas

## Success Criteria
✅ Parquet file loaded successfully  
✅ Schema displayed correctly  
✅ Metadata columns added  
✅ Data saved to bronze layer in S3
