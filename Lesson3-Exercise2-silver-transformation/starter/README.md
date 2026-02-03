# Exercise 2: Silver Layer Transformation - Student Instructions

## Objective
Clean, enrich, and join data to create the Silver layer with quality transformations.

## What You'll Learn
- Filter null and invalid data
- Add derived columns (date truncation)
- Flatten nested JSON structures with explode
- Join DataFrames in Spark
- Remove duplicates
- Write partitioned data

## Prerequisites
- Exercise 1 completed
- Pre-configured Docker container with PySpark environment
- Understanding of Spark transformations
- Knowledge of data quality principles

## Step-by-Step Instructions

### Step 1: Clean Orders Data
Filter invalid records and add date column:
```python
silver_orders = bronze_orders.filter(
    col("order_value").isNotNull() & 
    col("user_id").isNotNull() &
    (col("order_value") > 0)
).withColumn("order_date", date_trunc("day", col("order_timestamp")))
```

### Step 2: Flatten Clickstream Events
Explode nested arrays:
```python
silver_events = bronze_clicks.select(
    col("user_id"),
    explode(col("events.actions")).alias("action"),
    col("events.page").alias("page")
).filter(col("user_id").isNotNull())
```

### Step 3: Join Orders with Events
Left outer join to preserve all orders:
```python
silver = silver_orders.join(silver_events, "user_id", "left_outer") \
                     .dropDuplicates(["order_id"])
```

### Step 4: Write Partitioned Data
Save with date partitioning:
```python
silver.write.mode("overwrite").partitionBy("order_date").parquet("output/silver/")
```

## Understanding Silver Layer
- **Data Quality**: Remove nulls, invalid values
- **Enrichment**: Add derived columns
- **Joins**: Combine related datasets
- **Deduplication**: Remove duplicate records
- **Partitioning**: Organize for efficient queries

## Expected Output
```
=== SILVER LAYER: DATA CLEANING AND ENRICHMENT ===

Silver Orders Count: 950
Silver Events Count: 1200
Silver Joined Count: 950
Processing Time: 3.456s

Sample Silver Data:
+--------+-------+----------+-----------+----------+----------+
|order_id|user_id|product_id|order_value|action    |order_date|
+--------+-------+----------+-----------+----------+----------+
|1       |user_45|prod_12   |125.50     |view      |2024-01-15|
|2       |user_23|prod_8    |89.99      |add_to_cart|2024-01-15|
+--------+-------+----------+-----------+----------+----------+

Silver data written to output/silver/
```

## Verification
Check partitioned output:
```bash
ls -la output/silver/order_date=*/
```

## Common Issues
- **Join explosion**: Use dropDuplicates after join
- **Null values**: Filter before join
- **Partition skew**: Check date distribution

## Success Criteria
✅ Invalid data filtered  
✅ Date column added  
✅ Nested JSON flattened  
✅ Join completed successfully  
✅ Data partitioned by date
