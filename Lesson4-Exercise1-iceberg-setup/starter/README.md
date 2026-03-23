# Exercise 1: Create S3 Tables with Iceberg - Setup

## Quick Start

Run the setup script to create the S3 table bucket and namespace:
```bash
python solution/complete_setup.py
```

This script:
1. ✅ Creates table bucket: `swiftshop-analytics-tables`
2. ✅ Creates namespace: `swiftshop`
3. ✅ Creates table: `silver_orders` (Iceberg format, empty for Exercise 2)

## Before Running

Update `ATHENA_BUCKET_NAME` in `solution/complete_setup.py` if needed:
```python
ATHENA_BUCKET_NAME = 's3://swiftshop-data-lake/athena-results/'
```

## Table Schema

**silver_orders**
- order_id: STRING
- user_id: STRING
- product_id: STRING
- order_value: DOUBLE
- order_date: TIMESTAMP
- status: STRING
- processed_at: TIMESTAMP

## SQL Starter

The `starter/create_table.sql` file contains SQL templates for:
1. Creating the silver_orders table via Athena
2. Inserting sample data
3. Querying and verifying the table

## Verification

Query in Athena (use catalog: `s3tablescatalog/swiftshop-analytics-tables`):
```sql
DESCRIBE swiftshop.silver_orders;
```

## Success Criteria
✅ Script completes without errors
✅ Table bucket and namespace created
✅ silver_orders table exists (empty, ready for Exercise 2)
