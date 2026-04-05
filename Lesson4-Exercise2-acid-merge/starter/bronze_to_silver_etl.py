"""
Exercise 2 Starter: ACID Transactions and MERGE Operations
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tables") \
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.s3tables.glue.id", "633948902050:s3tablescatalog/swiftshop-analytics-tables") \
    .config("spark.sql.catalog.s3tables.warehouse", "s3://swiftshop-analytics-tables/bucket/swiftshop-analytics-tables") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

NAMESPACE = "swiftshop"
SILVER_TABLE = "silver_orders"

print("=== Starting Bronze to Silver ETL ===")

# TODO: Read bronze parquet from S3
bronze_df = # YOUR CODE HERE

print(f"Bronze records: {bronze_df.count()}")

# TODO: ETL Transformations
# - Filter null order_value
# - Clean negative order_value to 0
# - Replace null status with "unknown"
# - Add processed_at timestamp
# - Select and rename columns
silver_df = bronze_df  # YOUR CODE HERE

print(f"Silver records after transformation: {silver_df.count()}")

# TODO: Write to S3 Tables as Iceberg
# Hint: Use silver_df.writeTo() with .using("iceberg")
# YOUR CODE HERE

print(f"✓ ETL Complete: Data written to s3tables.{NAMESPACE}.{SILVER_TABLE}")

job.commit()
