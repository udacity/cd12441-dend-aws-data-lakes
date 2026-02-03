# Exercise 1: Bronze Layer Ingestion with PySpark - Student Instructions

## Objective
Load raw structured and unstructured data into Bronze layer using PySpark.

## What You'll Learn
- Initialize PySpark sessions
- Read Parquet files with Spark
- Read JSON files with Spark
- Display schemas and sample data
- Understand Bronze layer principles

## Prerequisites
- Pre-configured Docker container with PySpark environment
- All dependencies (PySpark, pandas) pre-installed
- Sample data files available in `data/` directory

## Step-by-Step Instructions

### Step 1: Initialize Spark Session
Create a Spark session with configuration:
```python
spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

### Step 2: Load Structured Data
Read Parquet file:
```python
bronze_orders = spark.read.parquet("data/orders.parquet")
```

### Step 3: Load Unstructured Data
Read JSON lines file:
```python
bronze_clicks = spark.read.json("data/clickstream.json")
```

### Step 4: Display Schemas
Print schema and count:
```python
bronze_orders.printSchema()
print(f"Count: {bronze_orders.count()}")
```

### Step 5: Show Sample Data
Display first 5 records:
```python
bronze_orders.show(5)
bronze_clicks.show(5, truncate=False)
```

## Expected Output
```
=== BRONZE LAYER: RAW DATA INGESTION ===

Bronze Orders Schema:
root
 |-- order_id: long
 |-- user_id: string
 |-- product_id: string
 |-- order_value: double
 |-- order_timestamp: timestamp

Bronze Orders Count: 1000

Bronze Clickstream Schema:
root
 |-- user_id: string
 |-- timestamp: string
 |-- events: struct
 |    |-- page: string
 |    |-- actions: array

Bronze Clickstream Count: 500

Bronze Load Time: 2.345s
```

## Common Issues
- **Spark not found**: Install with `pip install pyspark`
- **File not found**: Verify data/ directory exists
- **Memory error**: Reduce data size or increase Spark memory

## Success Criteria
✅ Spark session initialized  
✅ Parquet data loaded  
✅ JSON data loaded  
✅ Schemas displayed correctly  
✅ Sample data shown
