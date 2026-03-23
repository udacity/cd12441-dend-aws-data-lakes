# Exercise 2: Unstructured Data Ingestion - Student Instructions

## Objective
Load unstructured JSON data into the bronze layer, understanding schema-on-read principles.

## What You'll Learn
- Load JSON lines format using pandas
- Understand schema-on-read (schema inferred at read time)
- Handle nested JSON structures with `json_normalize`
- Identify variable/optional fields
- Compare structured vs unstructured approaches

## Prerequisites
- Exercise 1 completed
- Pre-configured Docker container with all dependencies
- Sample data: `data/clickstream.json` available locally
- All Python packages (boto3, pandas, python-dotenv) pre-installed

## Step-by-Step Instructions

### Step 1: Initialize S3 Client
```python
s3_client = boto3.client('s3')
```

### Step 2: Load JSON Lines
```python
clickstream_df = pd.read_json(LOCAL_DATA_PATH, lines=True)
```

### Step 3: Examine Inferred Schema
```python
clickstream_df.dtypes.to_string()
```

### Step 4: Preview Data
```python
clickstream_df.head(5).to_string()
```

### Step 5: Expand Nested Metadata
Use `json_normalize` to flatten the nested `metadata` column:
```python
metadata_df = pd.json_normalize(clickstream_df['metadata'])
clickstream_expanded['browser'] = metadata_df['browser']
clickstream_expanded['device'] = metadata_df['device']
clickstream_expanded['referrer'] = metadata_df['referrer']
```

### Step 6: Identify Variable Fields
Count events with and without `product_id`:
```python
with_product = clickstream_df['product_id'].notna().sum()
without_product = clickstream_df['product_id'].isna().sum()
```

### Step 7: Write to S3
```python
buffer = BytesIO()
clickstream_df.to_parquet(buffer, index=False)
buffer.seek(0)
s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH, Body=buffer.getvalue())
```

### Step 8: Verify S3 Write
```python
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_BRONZE_PATH)
bronze_df = pd.read_parquet(BytesIO(response['Body'].read()))
```

### Step 9: Compare with Exercise 1
Load orders data from S3 and compare structured vs unstructured:
```python
response = s3_client.get_object(Bucket=BUCKET_NAME, Key='orders/orders.parquet')
orders_df = pd.read_parquet(BytesIO(response['Body'].read()))
```

## Understanding Schema-on-Read
- **Schema inferred at read time**: Flexible structure
- **Nested structures**: Objects within objects
- **Variable fields**: Not all records have same fields
- **Trade-off**: Flexibility vs type safety

## JSON vs Parquet Comparison
| Aspect | JSON (Unstructured) | Parquet (Structured) |
|--------|---------------------|----------------------|
| Schema | Flexible, inferred | Fixed, enforced |
| Nested data | Native support | Requires flattening |
| Storage | Text-based, larger | Binary, compressed |
| Query speed | Slower | Faster |

## Expected Output
```
[Step 2] Loading unstructured data (clickstream.json)...
✓ Data loaded in 0.12 seconds
  Events: 5,000
  Columns: 7

[Step 6] Schema Flexibility - Variable Fields...
  Events with product_id: 3,500 (70.0%)
  Events without product_id: 1,500 (30.0%)

[Step 8] Verifying S3 Bronze Layer...
✓ Verification successful
  Match: ✓
```

## Common Issues
- **JSON parse error**: Check for malformed JSON
- **Missing nested fields**: Use `json_normalize` for nested dicts
- **Variable fields**: Handle optional fields with `.notna()`

## Success Criteria
✅ JSON lines parsed successfully
✅ Nested structure expanded with json_normalize
✅ Variable fields identified
✅ Data saved to bronze layer
✅ Comparison with structured data completed
