# Exercise 1: Silver Layer Transformation - Student Instructions

## Objective
Clean and deduplicate bronze data to create the Silver layer using PySpark.

## What You'll Learn
- Filter null and invalid data
- Rename columns for business clarity
- Cast data types
- Remove duplicate records
- Write cleaned data as Parquet

## Prerequisites
- Pre-configured Docker container with PySpark environment
- Sample data: `data/orders.parquet` and `data/clickstream.json`
- Understanding of Spark transformations

## Step-by-Step Instructions

### Step 1: Clean Orders Data
Filter invalid records, rename columns, and deduplicate:
```python
silver_orders = bronze_orders.filter(
    col("order_value").isNotNull() & 
    col("user_id").isNotNull() &
    (col("order_value") > 0)
).withColumn("order_date", col("order_date").cast("date")) \
 .withColumnRenamed("order_value", "revenue") \
 .withColumnRenamed("user_id", "customer_id") \
 .dropDuplicates(["order_id"])
```

### Step 2: Clean Clickstream
Select unique users from clickstream:
```python
silver_clicks = bronze_clicks.select(
    col("user_id")
).filter(col("user_id").isNotNull()).dropDuplicates()
```

### Step 3: Show Sample Data
```python
silver_orders.select("order_id", "customer_id", "product_id", "revenue", "status", "order_date").show(5)
silver_clicks.show(5)
```

### Step 4: Write Output
```python
silver_orders.write.mode("overwrite").parquet("output/silver_orders.parquet")
silver_clicks.write.mode("overwrite").parquet("output/silver_clicks.parquet")
```

## Understanding Silver Layer
- **Data Quality**: Remove nulls, invalid values, duplicates
- **Column Renaming**: `order_value` → `revenue`, `user_id` → `customer_id`
- **Type Casting**: Ensure `order_date` is proper date type
- **Deduplication**: Remove duplicate `order_id` records

## Expected Output
```
=== SILVER LAYER: DATA CLEANING AND ENRICHMENT ===

Silver Orders Count: 9,270
Silver Clicks Count: 4,500
Processing Time: 1.234s

Sample Silver Orders:
+----------+-----------+----------+-------+---------+----------+
|  order_id|customer_id|product_id|revenue|   status|order_date|
+----------+-----------+----------+-------+---------+----------+
|order_00000|  user_00123|  prod_045| 234.56|completed|2025-03-15|
+----------+-----------+----------+-------+---------+----------+
```

## Running Your Code
```bash
python silver_transformation_starter.py
```

## Common Issues
- **Null values**: Filter before renaming
- **Duplicate records**: Use `dropDuplicates` on primary key
- **Type errors**: Cast columns before comparisons

## Success Criteria
✅ Invalid data filtered (nulls, negative values)
✅ Columns renamed (`revenue`, `customer_id`)
✅ Date column cast to date type
✅ Duplicates removed
✅ Silver data written to output/
