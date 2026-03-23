# Exercise 3: Bronze Layer Organization - Student Instructions

## Objective
Organize bronze layer with hierarchical structure and date partitioning for efficient querying.

## What You'll Learn
- Design hierarchical bronze layer structure
- Implement date-based partitioning
- Separate structured vs unstructured data
- Add metadata columns for lineage tracking
- Create metadata JSON files for datasets

## Prerequisites
- Exercise 1 & 2 completed (data in bronze)
- Pre-configured Docker container with all dependencies
- Understanding of partitioning concepts

## Step-by-Step Instructions

### Step 1: Initialize S3 Client
```python
s3_client = boto3.client('s3')
```

### Step 2: Review Current Structure
Understand the flat structure from Exercises 1 & 2:
```
s3://bucket/
├── orders/orders.parquet
└── clickstream/clickstream.parquet
```

### Step 3: Load Existing Data
List and read parquet files from S3:
```python
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix='orders/')
orders_key = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')][0]
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=orders_key)
orders_df = pd.read_parquet(BytesIO(response['Body'].read()))
```

### Step 4: Add Metadata Columns
```python
orders_df['ingestion_timestamp'] = datetime.now()
orders_df['source_system'] = 'postgresql'
orders_df['data_type'] = 'structured'
orders_df['ingestion_date'] = pd.to_datetime(orders_df['order_date']).dt.date
```

### Step 5: Write Date Partitions
```python
for date, group in orders_df.groupby('ingestion_date'):
    partition_path = f'structured/orders/raw/date={date}/orders.parquet'
    buffer = BytesIO()
    group.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=partition_path, Body=buffer.getvalue())
```

### Step 6: Create Metadata Files
```python
orders_metadata = {
    "dataset_name": "orders",
    "data_type": "structured",
    "source_system": "postgresql",
    "row_count": len(orders_df),
    "columns": len(orders_df.columns),
    "created_at": datetime.now().isoformat()
}
s3_client.put_object(
    Bucket=BUCKET_NAME,
    Key='structured/orders/metadata.json',
    Body=json.dumps(orders_metadata, indent=2)
)
```

### Step 7: Demonstrate Partition Benefits
Read a single partition to show efficiency:
```python
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=partition_path)
partition_df = pd.read_parquet(BytesIO(response['Body'].read()))
```

## Target Structure
```
s3://bucket/
├── structured/
│   └── orders/
│       ├── raw/
│       │   └── date=2026-01-15/
│       │       └── orders.parquet
│       └── metadata.json
└── unstructured/
    └── clickstream/
        ├── raw/
        │   └── date=2026-01-15/
        │       └── clickstream.parquet
        └── metadata.json
```

## Expected Output
```
[Step 4] Reorganizing Structured Data (Orders)...
  Loaded 10,000 orders from existing bronze
  Writing to organized structure with date partitions...
✓ Structured data reorganized
  Partitions created: 365

[Step 5] Reorganizing Unstructured Data (Clickstream)...
  Loaded 5,000 events from existing bronze
✓ Unstructured data reorganized

[Step 6] Creating Metadata Files...
✓ Orders metadata: 10,000 rows, 10 columns
✓ Clickstream metadata: 5,000 events, 11 columns

[Step 7] Demonstrating Partition Benefits...
✓ Partition query completed
  Benefit: Only reads relevant partition, not entire dataset
```

## Common Issues
- **No data found**: Run Exercise 1 & 2 first
- **Date parsing errors**: Ensure correct datetime format
- **Empty partitions**: Check groupby logic

## Success Criteria
✅ Data partitioned by date
✅ Hierarchical structure created
✅ Structured/unstructured separated
✅ Metadata files created
✅ Partition query demonstrated
