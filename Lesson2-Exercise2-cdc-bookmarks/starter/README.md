# Exercise 2: CDC with Bookmarks - Student Instructions

## Objective
Implement Change Data Capture (CDC) to incrementally load only new or updated records using timestamp bookmarks.

## What You'll Learn
- Build incremental queries with WHERE clauses
- Use job parameters for bookmark tracking
- Calculate max timestamp for next run
- Implement append-only loading patterns

## Prerequisites
- Exercise 1 completed (full table load working)
- Pre-configured AWS Glue environment
- PostgreSQL orders table with `updated_at` timestamp column
- Understanding of incremental vs full loads

## Step-by-Step Instructions

### Step 1: Understand the Bookmark Pattern
The bookmark is a timestamp passed as a job parameter. Only records with `updated_at > bookmark` are loaded.

### Step 2: Build Incremental Query
Complete the query string:
```python
query = f"(SELECT * FROM orders WHERE updated_at > '{args['bookmark']}') as incremental_query"
```
This creates a subquery that filters records.

### Step 3: Configure Connection with Query
Use the `query` variable in the `dbtable` parameter instead of a table name.

### Step 4: Calculate Next Bookmark
After loading data, find the maximum `updated_at` value:
```python
max_updated_at = df.agg(spark_max("updated_at")).collect()[0][0]
```
This becomes the bookmark for the next run.

### Step 5: Use Append Mode
Change write mode from `"overwrite"` to `"append"` to add new records without deleting existing data.

## Testing Your CDC Implementation

### Initial Load
```bash
aws glue start-job-run \
  --job-name cdc-bookmarks-exercise \
  --arguments '--bookmark=1900-01-01 00:00:00'
```

### Update Source Data
```sql
UPDATE orders 
SET order_value = 200.00, updated_at = NOW() 
WHERE order_id = 123;
```

### Incremental Load
```bash
aws glue start-job-run \
  --job-name cdc-bookmarks-exercise \
  --arguments '--bookmark=2024-01-15 10:30:00'
```

## Expected Output
```
Starting CDC ingestion with bookmark: 2024-01-15 10:30:00
Records retrieved: 25
Records processed: 25
Next bookmark: 2024-01-15 14:22:33
```

## Common Issues
- **All records loaded**: Check WHERE clause syntax
- **No records found**: Bookmark might be too recent
- **Duplicate records**: Using overwrite instead of append

## Verification
```sql
-- Check for incremental files
SELECT COUNT(*) FROM bronze_orders;

-- Verify updated records
SELECT * FROM bronze_orders WHERE order_id = 123;
```

## Success Criteria
✅ Only new/updated records loaded  
✅ Bookmark correctly calculated  
✅ Append mode working (no data loss)  
✅ Can run multiple times with different bookmarks
