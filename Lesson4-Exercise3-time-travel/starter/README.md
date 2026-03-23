# Exercise 3: Time Travel and Schema Evolution - Student Instructions

## Objective
Use Iceberg time travel to query historical data and evolve table schemas without downtime.

## What You'll Learn
- Query current and historical table versions
- View table snapshot history
- Compare data across versions
- Add columns to existing tables (schema evolution)
- Understand S3 Tables automatic compaction

## Prerequisites
- Exercise 1 & 2 completed (silver_orders table with data)
- AWS Glue 5.0 environment
- Iceberg tables with at least one snapshot

## Step-by-Step Instructions

### Step 1: Query Current Version
```python
COLS = "order_id, user_id, product_id, order_value, order_date, status, processed_at"
current_data = spark.sql(f"SELECT {COLS} FROM {TABLE} LIMIT 5")
current_data.show()
```

### Step 2: Show Table History
```python
table_history = spark.sql(f"""
SELECT snapshot_id, made_current_at, is_current_ancestor 
FROM {TABLE}.history 
ORDER BY made_current_at DESC
""")
table_history.show()
```

### Step 3: Query Historical Version
Get the first snapshot ID and query it:
```python
first_snapshot = spark.sql(f"""
SELECT snapshot_id FROM {TABLE}.history 
ORDER BY made_current_at ASC LIMIT 1
""").collect()[0][0]

historical_data = spark.sql(f"""
SELECT {COLS} FROM {TABLE} 
FOR VERSION AS OF {first_snapshot} 
LIMIT 5
""")
```

### Step 4: Compare Versions
```python
current_stats = spark.sql(f"""
    SELECT COUNT(*) as cnt, AVG(order_value) as avg_val FROM {TABLE}
""").collect()[0]

historical_stats = spark.sql(f"""
    SELECT COUNT(*) as cnt, AVG(order_value) as avg_val 
    FROM {TABLE} FOR VERSION AS OF {first_snapshot}
""").collect()[0]
```

### Step 5: Schema Evolution
Add a new column without downtime:
```python
spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN customer_segment STRING")
```

### Step 6: Automatic Compaction
S3 Tables handles file compaction automatically — no manual `rewrite_data_files` needed.

## Configuration
The starter uses this Spark/Iceberg configuration:
```python
TABLE = "swiftshop.silver_orders"

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tables") \
    .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.s3tables.glue.id", "ACCOUNT_ID:s3tablescatalog/swiftshop-analytics-tables") \
    .getOrCreate()
```

## Expected Output
```
=== TIME TRAVEL AND SCHEMA EVOLUTION ===

Current version:
+--------+-------+----------+-----------+-------------------+------+-------------------+
|order_id|user_id|product_id|order_value|         order_date|status|       processed_at|
+--------+-------+----------+-----------+-------------------+------+-------------------+

Table history:
+-----------+-------------------+--------------------+
|snapshot_id|   made_current_at |is_current_ancestor |
+-----------+-------------------+--------------------+

Growth analysis:
  Current count: 9480, Historical count: 9480, Growth: 0
  Current avg: 254.32, Historical avg: 254.32

✓ Column 'customer_segment' added
✓ S3 Tables handles file compaction automatically
```

## Common Issues
- **Version not found**: Check snapshot_id from `.history` table
- **Schema mismatch**: Old versions don't have new columns (returns NULL)
- **Column already exists**: ALTER TABLE ADD COLUMN will error if column exists

## Success Criteria
✅ Current version queried
✅ Table history displayed
✅ Historical version accessed via snapshot_id
✅ Version comparison completed
✅ Schema evolved (column added)
