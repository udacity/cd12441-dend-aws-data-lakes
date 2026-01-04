"""
Exercise 4 Starter Code: S3 Tables with Iceberg and Time Travel
Student Name: _______________
Date: _______________

Instructions:
1. Complete Lake Formation setup from setup/lake_formation_setup.md
2. Complete the TODO sections for Iceberg configuration
3. Deploy as AWS Glue 5.0 ETL job (Spark 3.5, 10+ DPU)
4. Test ACID operations and time travel queries
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, lit

# TODO: Get job arguments
args = # YOUR CODE HERE

# TODO: Initialize Spark and Glue contexts
sc = # YOUR CODE HERE
glueContext = # YOUR CODE HERE
spark = # YOUR CODE HERE

print("=== EXERCISE 4: S3 TABLES WITH ICEBERG ===")

# TODO: Configure Spark for Iceberg and S3 Tables integration
# Set the required Spark configurations for Iceberg extensions
spark.conf.set("spark.sql.extensions", # YOUR CODE HERE)
spark.conf.set("spark.sql.catalog.s3tables_catalog", # YOUR CODE HERE)
spark.conf.set("spark.sql.catalog.s3tables_catalog.glue.endpoint", # YOUR CODE HERE)
spark.conf.set("spark.sql.catalog.s3tables_catalog.glue.region", # YOUR CODE HERE)
spark.conf.set("spark.sql.catalog.s3tables_catalog.warehouse", # YOUR CODE HERE)

# TODO: Initialize Glue job
job = # YOUR CODE HERE
# YOUR CODE HERE - job.init()

print("Iceberg and S3 Tables configuration completed")

# STEP 1: Create Database and Tables
print("\nSTEP 1: Creating S3 Tables Database and Schema")
print("-" * 50)

# TODO: Create database in S3 Tables catalog
spark.sql(# YOUR CODE HERE)

# TODO: Create bronze_orders table with Iceberg format
# Include columns: order_id, user_id, product_id, order_value, order_date
# Partition by days(order_date)
spark.sql("""
# YOUR CODE HERE - CREATE TABLE statement
""")

# TODO: Create silver_orders table for CDC operations
spark.sql("""
# YOUR CODE HERE - CREATE TABLE statement for silver layer
""")

print("S3 Tables created successfully")

# STEP 2: Load Bronze Data
print("\nSTEP 2: Loading Bronze Data to Iceberg Tables")
print("-" * 50)

# TODO: Read bronze data from S3
bronze_df = glueContext.create_dynamic_frame.from_options(
    # YOUR CODE HERE - connection type and options
).toDF()

# TODO: Add metadata columns for tracking
bronze_df = bronze_df.withColumn("ingestion_timestamp", # YOUR CODE HERE) \
                    .withColumn("data_source", # YOUR CODE HERE)

print(f"Bronze records to load: {bronze_df.count()}")

# TODO: Write to Iceberg table using append mode
bronze_df.write.format(# YOUR CODE HERE) \
              .mode(# YOUR CODE HERE) \
              .saveAsTable(# YOUR CODE HERE)

print("Bronze data loaded to Iceberg table")

# STEP 3: CDC Processing with MERGE
print("\nSTEP 3: CDC Processing with MERGE Operations")
print("-" * 50)

# TODO: Read CDC updates from S3
updates_df = glueContext.create_dynamic_frame.from_options(
    # YOUR CODE HERE - read CDC updates
).toDF()

# TODO: Create temporary view for MERGE operation
updates_df.createOrReplaceTempView(# YOUR CODE HERE)

print(f"CDC updates to process: {updates_df.count()}")

# TODO: Execute MERGE statement for CDC processing
# MERGE INTO silver table USING updates ON order_id match
# WHEN MATCHED THEN UPDATE, WHEN NOT MATCHED THEN INSERT
spark.sql("""
# YOUR CODE HERE - MERGE statement
""")

print("CDC MERGE operation completed")

# STEP 4: Time Travel Queries
print("\nSTEP 4: Time Travel and Historical Analysis")
print("-" * 50)

# TODO: Query current version of the table
current_data = spark.sql(# YOUR CODE HERE)
print("Current table version:")
current_data.show(5)

# TODO: Query historical version (version 1)
historical_data = spark.sql(# YOUR CODE HERE)
print("Historical table version (v1):")
historical_data.show(5)

# TODO: Show table history and snapshots
table_history = spark.sql(# YOUR CODE HERE)
print("Table history:")
table_history.show()

# STEP 5: Schema Evolution
print("\nSTEP 5: Schema Evolution")
print("-" * 50)

# TODO: Add new column to existing table
spark.sql(# YOUR CODE HERE)

# TODO: Add new partition field
spark.sql(# YOUR CODE HERE)

print("Schema evolution completed")

# STEP 6: Performance Optimization
print("\nSTEP 6: Performance Optimization")
print("-" * 50)

# TODO: Compact small files
spark.sql(# YOUR CODE HERE)

# TODO: Update table statistics
spark.sql(# YOUR CODE HERE)

print("Table optimization completed")

# STEP 7: Query Performance Analysis
print("\nSTEP 7: Query Performance Analysis")
print("-" * 50)

# TODO: Execute partition pruning query
partition_query = spark.sql(# YOUR CODE HERE)
print("Partition pruning results:")
partition_query.show()

# TODO: Execute time-based aggregation
time_agg = spark.sql(# YOUR CODE HERE)
print("Time-based aggregation:")
time_agg.show()

# TODO: Commit the job
# YOUR CODE HERE

print("\n=== S3 TABLES EXERCISE COMPLETED ===")
print("✓ Iceberg tables created with ACID support")
print("✓ CDC processing with MERGE operations")
print("✓ Time travel queries executed")
print("✓ Schema evolution demonstrated")
print("✓ Performance optimization applied")

"""
Verification Steps:
1. Check S3 Tables in Lake Formation console
2. Query tables using Athena:
   SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders LIMIT 10;
3. Verify time travel:
   SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders FOR VERSION AS OF 1;
4. Check table metadata:
   DESCRIBE EXTENDED s3tables_catalog.cloudmart_db.bronze_orders;
"""
