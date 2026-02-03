# Exercise 2: Unstructured Data Ingestion - Student Instructions

## Objective
Load unstructured JSON data from S3 into the bronze layer, understanding schema-on-read principles.

## What You'll Learn
- Load JSON lines format from S3
- Understand schema-on-read (schema inferred at read time)
- Handle nested JSON structures
- Flatten nested data for analysis
- Compare structured vs unstructured approaches

## Prerequisites
- Exercise 1 completed
- Pre-configured Docker container with all dependencies
- Sample data: `clickstream.json` available in source S3 bucket
- All Python packages pre-installed

## Step-by-Step Instructions

### Step 1: Load JSON Lines from S3
Parse JSON lines file:
```python
json_obj = s3.get_object(Bucket=source_bucket, Key='raw/clickstream.json')
json_content = json_obj['Body'].read().decode('utf-8')
raw_logs = [json.loads(line) for line in json_content.strip().split('\n')]
```

### Step 2: Examine JSON Structure
Inspect the nested structure:
```python
print(f"Sample record keys: {raw_logs[0].keys()}")
print(f"Sample record:\n{json.dumps(raw_logs[0], indent=2)}")
```

### Step 3: Flatten Nested JSON
Extract nested fields:
```python
flattened_data = []
for log in raw_logs:
    flat_record = {
        'event_id': log['event_id'],
        'user_id': log['user_id'],
        'event_type': log['event_type'],
        'timestamp': log['timestamp'],
        'browser': log.get('metadata', {}).get('browser'),
        'device': log.get('metadata', {}).get('device'),
        'product_id': log.get('product_id')  # Optional field
    }
    flattened_data.append(flat_record)

clickstream_df = pd.DataFrame(flattened_data)
```

### Step 4: Save to Bronze Layer
Write as Parquet:
```python
parquet_buffer = BytesIO()
clickstream_df.to_parquet(parquet_buffer, index=False)
s3.put_object(
    Bucket=bronze_bucket,
    Key='bronze/unstructured/clickstream/clickstream.parquet',
    Body=parquet_buffer.getvalue()
)
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
=== EXERCISE 2: BRONZE LAYER - UNSTRUCTURED DATA ===

STEP 1: LOAD UNSTRUCTURED DATA (JSON)
Sample record keys: dict_keys(['event_id', 'user_id', 'event_type', 'timestamp', 'metadata', 'product_id'])
Sample record:
{
  "event_id": "evt_001",
  "user_id": "user_123",
  "event_type": "page_view",
  "timestamp": "2024-01-15T10:30:00",
  "metadata": {
    "browser": "Chrome",
    "device": "mobile"
  }
}

STEP 2: FLATTEN NESTED JSON (SCHEMA-ON-READ)
Flattened columns: ['event_id', 'user_id', 'event_type', 'timestamp', 'browser', 'device', 'product_id']

STEP 3: SAVE TO BRONZE LAYER
✓ Data saved to bronze/unstructured/clickstream/clickstream.parquet
```

## Common Issues
- **JSON parse error**: Check for malformed JSON
- **Missing nested fields**: Use `.get()` with defaults
- **Variable fields**: Handle optional fields gracefully

## Success Criteria
✅ JSON lines parsed successfully  
✅ Nested structure flattened  
✅ Variable fields handled  
✅ Data saved to bronze layer
