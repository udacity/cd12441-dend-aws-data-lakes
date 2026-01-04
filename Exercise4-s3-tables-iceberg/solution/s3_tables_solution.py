"""
Exercise 4 Solution: S3 Tables with Iceberg and Time Travel
Complete AWS Glue 5.0 implementation with ACID transactions and schema evolution
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, lit, date_trunc

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== EXERCISE 4: S3 TABLES WITH ICEBERG ===")

# Configure Spark for Iceberg and S3 Tables
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.s3tables_catalog.glue.endpoint", "https://glue.us-east-1.amazonaws.com")
spark.conf.set("spark.sql.catalog.s3tables_catalog.glue.region", "us-east-1")
spark.conf.set("spark.sql.catalog.s3tables_catalog.warehouse", "s3://cloudmart-s3tables-metadata/warehouse")

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Iceberg and S3 Tables configuration completed")

# STEP 1: Create Database and Tables
print("\nSTEP 1: Creating S3 Tables Database and Schema")
print("-" * 50)

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS s3tables_catalog.cloudmart_db")

# Create bronze_orders table
spark.sql("""
CREATE TABLE IF NOT EXISTS s3tables_catalog.cloudmart_db.bronze_orders (
    order_id bigint,
    user_id string,
    product_id string,
    order_value double,
    order_date timestamp,
    ingestion_timestamp timestamp,
    data_source string
) USING iceberg 
PARTITIONED BY (days(order_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
""")

# Create silver_orders table for CDC
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

print("S3 Tables created successfully")

# STEP 2: Load Bronze Data
print("\nSTEP 2: Loading Bronze Data to Iceberg Tables")
print("-" * 50)

# Read bronze data from S3
bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://cloudmart/bronze/orders/"]}
).toDF()

# Add metadata columns
bronze_df = bronze_df.withColumn("ingestion_timestamp", current_timestamp()) \
                    .withColumn("data_source", lit("s3_bronze_layer"))

print(f"Bronze records to load: {bronze_df.count()}")

# Write to Iceberg table
bronze_df.write.format("iceberg") \
              .mode("append") \
              .saveAsTable("s3tables_catalog.cloudmart_db.bronze_orders")

print("Bronze data loaded to Iceberg table")

# STEP 3: CDC Processing with MERGE
print("\nSTEP 3: CDC Processing with MERGE Operations")
print("-" * 50)

# Simulate CDC updates (in real scenario, read from CDC source)
updates_df = bronze_df.limit(100) \
                     .withColumn("updated_at", current_timestamp()) \
                     .withColumn("status", lit("updated")) \
                     .withColumn("order_value", col("order_value") * 1.1)

updates_df.createOrReplaceTempView("cdc_updates")

print(f"CDC updates to process: {updates_df.count()}")

# Execute MERGE for CDC processing
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

print("CDC MERGE operation completed")

# STEP 4: Time Travel Queries
print("\nSTEP 4: Time Travel and Historical Analysis")
print("-" * 50)

# Query current version
current_data = spark.sql("SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders LIMIT 5")
print("Current table version:")
current_data.show()

# Query historical version (snapshot ID or timestamp)
historical_data = spark.sql("""
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders 
FOR VERSION AS OF 1 
LIMIT 5
""")
print("Historical table version (v1):")
historical_data.show()

# Show table history
table_history = spark.sql("""
SELECT snapshot_id, committed_at, summary 
FROM s3tables_catalog.cloudmart_db.bronze_orders.history 
ORDER BY committed_at DESC
""")
print("Table history:")
table_history.show()

# STEP 5: Schema Evolution
print("\nSTEP 5: Schema Evolution")
print("-" * 50)

# Add new column
spark.sql("""
ALTER TABLE s3tables_catalog.cloudmart_db.bronze_orders 
ADD COLUMN customer_segment string
""")

# Add new partition field (requires table recreation for existing data)
spark.sql("""
ALTER TABLE s3tables_catalog.cloudmart_db.bronze_orders 
ADD PARTITION FIELD bucket(16, user_id)
""")

print("Schema evolution completed")

# STEP 6: Performance Optimization
print("\nSTEP 6: Performance Optimization")
print("-" * 50)

# Compact small files
spark.sql("CALL s3tables_catalog.system.rewrite_data_files('cloudmart_db.bronze_orders')")

# Update table statistics
spark.sql("ANALYZE TABLE s3tables_catalog.cloudmart_db.bronze_orders COMPUTE STATISTICS")

print("Table optimization completed")

# STEP 7: Query Performance Analysis
print("\nSTEP 7: Query Performance Analysis")
print("-" * 50)

# Partition pruning query
partition_query = spark.sql("""
SELECT order_date, COUNT(*) as order_count, SUM(order_value) as total_revenue
FROM s3tables_catalog.cloudmart_db.bronze_orders 
WHERE order_date >= current_date() - INTERVAL 7 DAYS
GROUP BY order_date
ORDER BY order_date DESC
""")
print("Partition pruning results:")
partition_query.show()

# Time-based aggregation with window functions
time_agg = spark.sql("""
SELECT 
    date_trunc('hour', ingestion_timestamp) as hour,
    COUNT(*) as records_ingested,
    AVG(order_value) as avg_order_value
FROM s3tables_catalog.cloudmart_db.bronze_orders 
GROUP BY date_trunc('hour', ingestion_timestamp)
ORDER BY hour DESC
LIMIT 24
""")
print("Time-based aggregation:")
time_agg.show()

# Advanced analytics with time travel comparison
comparison_query = spark.sql("""
WITH current_stats AS (
    SELECT COUNT(*) as current_count, AVG(order_value) as current_avg
    FROM s3tables_catalog.cloudmart_db.bronze_orders
),
historical_stats AS (
    SELECT COUNT(*) as historical_count, AVG(order_value) as historical_avg
    FROM s3tables_catalog.cloudmart_db.bronze_orders FOR VERSION AS OF 1
)
SELECT 
    current_count,
    historical_count,
    current_count - historical_count as growth,
    current_avg,
    historical_avg,
    current_avg - historical_avg as avg_change
FROM current_stats, historical_stats
""")
print("Growth analysis using time travel:")
comparison_query.show()

# Commit job
job.commit()

print("\n=== S3 TABLES EXERCISE COMPLETED ===")
print("✓ Iceberg tables created with ACID support")
print("✓ CDC processing with MERGE operations")
print("✓ Time travel queries executed")
print("✓ Schema evolution demonstrated")
print("✓ Performance optimization applied")

"""
Deployment Instructions:

1. Create Glue 5.0 Job:
aws glue create-job \
  --name "cloudmart-s3-tables-iceberg" \
  --role "AWSGlueServiceRole" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-scripts-bucket/s3_tables_solution.py",
    "PythonVersion": "3"
  }' \
  --glue-version "5.0" \
  --number-of-workers 10 \
  --worker-type "G.1X"

2. Run Job:
aws glue start-job-run --job-name "cloudmart-s3-tables-iceberg"

3. Verify with Athena:
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders LIMIT 10;
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders FOR VERSION AS OF 1;
DESCRIBE EXTENDED s3tables_catalog.cloudmart_db.bronze_orders;

4. Check S3 Tables Console:
- Navigate to Lake Formation > S3 Tables
- View table metadata and query history
- Monitor performance metrics
"""
