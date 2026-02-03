# Exercise 2: ACID Transactions and MERGE Operations - Student Instructions

## Objective
Implement ACID transactions using Iceberg MERGE operations for CDC processing.

## What You'll Learn
- Execute MERGE statements in Iceberg
- Handle CDC updates with UPSERT logic
- Implement ACID transaction guarantees
- Process incremental updates efficiently
- Understand MERGE performance optimization

## Prerequisites
- Exercise 1 completed (Iceberg tables created)
- Pre-configured AWS Glue 5.0 environment
- Understanding of CDC patterns
- Knowledge of SQL MERGE syntax
- CDC update data available in S3

## Step-by-Step Instructions

### Step 1: Create Silver Table
```python
spark.sql("""
CREATE TABLE IF NOT EXISTS s3tables_catalog.cloudmart_db.silver_orders (
    order_id bigint,
    user_id string,
    product_id string,
    order_value double,
    order_date timestamp,
    updated_at timestamp,
    status string
) USING iceberg 
PARTITIONED BY (days(order_date))
""")
```

### Step 2: Read CDC Updates
```python
updates_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://cloudmart/cdc/updates/"]}
).toDF()

updates_df.createOrReplaceTempView("cdc_updates")
```

### Step 3: Execute MERGE Operation
```python
spark.sql("""
MERGE INTO s3tables_catalog.cloudmart_db.silver_orders AS target
USING cdc_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET 
    target.order_value = source.order_value,
    target.updated_at = source.updated_at,
    target.status = source.status
WHEN NOT MATCHED THEN INSERT (
    order_id, user_id, product_id, order_value, order_date, updated_at, status
) VALUES (
    source.order_id, source.user_id, source.product_id, 
    source.order_value, source.order_date, source.updated_at, source.status
)
""")
```

### Step 4: Verify MERGE Results
```python
result = spark.sql("SELECT * FROM s3tables_catalog.cloudmart_db.silver_orders WHERE status = 'updated'")
result.show()
```

## Understanding MERGE
- **MATCHED**: Updates existing records
- **NOT MATCHED**: Inserts new records
- **ACID**: All-or-nothing transaction
- **Efficient**: Only processes changed records

## Expected Output
```
=== ACID TRANSACTIONS AND MERGE ===
CDC updates to process: 100
MERGE operation completed
Updated records: 75
Inserted records: 25
```

## Verification
```sql
SELECT status, COUNT(*) FROM s3tables_catalog.cloudmart_db.silver_orders GROUP BY status;
```

## Common Issues
- **Duplicate keys**: Ensure unique order_id in source
- **Schema mismatch**: Verify column names match
- **Transaction timeout**: Reduce batch size

## Success Criteria
✅ Silver table created  
✅ CDC updates loaded  
✅ MERGE executed successfully  
✅ Records updated and inserted  
✅ ACID guarantees maintained
