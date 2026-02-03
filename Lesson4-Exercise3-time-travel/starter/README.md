# Exercise 3: Time Travel and Schema Evolution - Student Instructions

## Objective
Use Iceberg time travel to query historical data and evolve table schemas without downtime.

## What You'll Learn
- Query historical table versions
- View table snapshot history
- Add columns to existing tables
- Compact small files for optimization
- Compare data across versions
- Understand schema evolution patterns

## Prerequisites
- Exercise 1 & 2 completed
- Pre-configured AWS Glue 5.0 environment
- Iceberg tables with multiple versions
- Understanding of versioning concepts

## Step-by-Step Instructions

### Step 1: Query Current Version
```python
current_data = spark.sql("SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders LIMIT 5")
current_data.show()
```

### Step 2: Query Historical Version
Use `FOR VERSION AS OF` to query specific snapshot:
```python
historical_data = spark.sql("""
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders 
FOR VERSION AS OF 1 
LIMIT 5
""")
historical_data.show()
```

### Step 3: View Table History
```python
table_history = spark.sql("""
SELECT snapshot_id, committed_at, summary 
FROM s3tables_catalog.cloudmart_db.bronze_orders.history 
ORDER BY committed_at DESC
""")
table_history.show()
```

### Step 4: Schema Evolution
Add new column without downtime:
```python
spark.sql("""
ALTER TABLE s3tables_catalog.cloudmart_db.bronze_orders 
ADD COLUMN customer_segment string
""")
```

### Step 5: Optimize Table
Compact small files:
```python
spark.sql("CALL s3tables_catalog.system.rewrite_data_files('cloudmart_db.bronze_orders')")
```

Update statistics:
```python
spark.sql("ANALYZE TABLE s3tables_catalog.cloudmart_db.bronze_orders COMPUTE STATISTICS")
```

### Step 6: Compare Versions
```python
comparison = spark.sql("""
WITH current_stats AS (
    SELECT COUNT(*) as current_count FROM s3tables_catalog.cloudmart_db.bronze_orders
),
historical_stats AS (
    SELECT COUNT(*) as historical_count 
    FROM s3tables_catalog.cloudmart_db.bronze_orders FOR VERSION AS OF 1
)
SELECT current_count, historical_count, current_count - historical_count as growth
FROM current_stats, historical_stats
""")
comparison.show()
```

## Understanding Time Travel
- **Snapshots**: Immutable table versions
- **Version AS OF**: Query specific snapshot
- **Timestamp AS OF**: Query at specific time
- **History**: View all snapshots

## Expected Output
```
=== TIME TRAVEL AND SCHEMA EVOLUTION ===

Current version:
+--------+-------+----------+-----------+
|order_id|user_id|product_id|order_value|
+--------+-------+----------+-----------+
|1       |user_45|prod_12   |125.50     |

Historical version (v1):
+--------+-------+----------+-----------+
|order_id|user_id|product_id|order_value|
+--------+-------+----------+-----------+
|1       |user_45|prod_12   |100.00     |

Table history:
+-----------+-------------------+
|snapshot_id|committed_at       |
+-----------+-------------------+
|123456789  |2024-01-15 10:30:00|
|123456788  |2024-01-15 09:00:00|

Growth analysis:
+-------------+----------------+------+
|current_count|historical_count|growth|
+-------------+----------------+------+
|1100         |1000            |100   |
```

## Verification
```sql
-- Query specific version
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders FOR VERSION AS OF 1;

-- Query at timestamp
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders 
FOR TIMESTAMP AS OF '2024-01-15 09:00:00';

-- View schema
DESCRIBE s3tables_catalog.cloudmart_db.bronze_orders;
```

## Common Issues
- **Version not found**: Check snapshot_id in history
- **Schema mismatch**: Old versions don't have new columns
- **Performance**: Compact files regularly

## Success Criteria
✅ Current version queried  
✅ Historical version accessed  
✅ Table history displayed  
✅ Schema evolved successfully  
✅ Files compacted  
✅ Version comparison completed
