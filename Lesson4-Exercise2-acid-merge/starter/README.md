# Exercise 2: Bronze to Silver ETL with ACID Transactions - Student Instructions

## Objective
Implement a Bronze to Silver ETL pipeline using PySpark on AWS Glue with Iceberg tables in S3 Tables.

## What You'll Learn
- Read bronze Parquet data from S3
- Apply ETL transformations (null handling, data cleaning, column renaming)
- Write to Iceberg tables in S3 Tables
- Configure Spark with Iceberg catalog extensions

## Prerequisites
- Exercise 1 completed (S3 table bucket and silver_orders table created)
- AWS Glue 5.0 environment
- Bronze data available in S3 (from Lesson 1)

## Step-by-Step Instructions

### Step 1: Configure Spark for Iceberg
The starter code includes the SparkSession configuration:
```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tables") \
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.s3tables.glue.id", "ACCOUNT_ID:s3tablescatalog/swiftshop-analytics-tables") \
    .config("spark.sql.catalog.s3tables.warehouse", "s3://swiftshop-analytics-tables/bucket/swiftshop-analytics-tables") \
    .getOrCreate()
```

### Step 2: Read Bronze Data
```python
bronze_df = spark.read.format("parquet") \
    .load("s3://YOUR-BRONZE-BUCKET/structured/orders/raw/")
```

### Step 3: Apply ETL Transformations
- Filter null `order_value`
- Clean negative values to 0
- Replace null `status` with "unknown"
- Add `processed_at` timestamp
- Select and rename columns

```python
silver_df = bronze_df \
    .filter(col("order_value").isNotNull()) \
    .withColumn("order_value_clean",
                when(col("order_value") < 0, 0)
                .otherwise(col("order_value"))) \
    .withColumn("status_clean",
                when(col("status").isNull(), "unknown")
                .otherwise(col("status"))) \
    .withColumn("processed_at", current_timestamp()) \
    .select(
        col("order_id"),
        col("user_id"),
        col("product_id"),
        col("order_value_clean").alias("order_value"),
        col("order_date").cast("timestamp").alias("order_date"),
        col("status_clean").alias("status"),
        col("processed_at")
    )
```

### Step 4: Write to S3 Tables
```python
silver_df.writeTo(f"{NAMESPACE}.{SILVER_TABLE}") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
```

## Expected Output
```
=== Starting Bronze to Silver ETL ===
Bronze records: 10,000
Silver records after transformation: 9,480
✓ ETL Complete: Data written to s3tables.swiftshop.silver_orders
```

## Verification
Query in Athena (catalog: `s3tablescatalog/swiftshop-analytics-tables`):
```sql
SELECT COUNT(*) FROM swiftshop.silver_orders;
SELECT status, COUNT(*) FROM swiftshop.silver_orders GROUP BY status;
```

## Common Issues
- **Catalog not found**: Verify glue.id matches your account
- **S3 path error**: Check bronze bucket path
- **Schema mismatch**: Ensure column names match table definition

## Success Criteria
✅ Bronze data read from S3
✅ Null values filtered
✅ Negative values cleaned
✅ Columns renamed and typed
✅ Data written to Iceberg table in S3 Tables
