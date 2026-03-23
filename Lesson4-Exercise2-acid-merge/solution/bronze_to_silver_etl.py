"""
Exercise 2: Bronze to Silver ETL with PySpark on AWS Glue
Reads bronze parquet from S3 and writes to silver Iceberg table in S3 Tables
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

# Read bronze parquet from S3
bronze_df = spark.read.format("parquet") \
    .load("s3://lakehouse-student-bronze-ae1254b1-cafe-479b-bce5-acb312f6d1c0/structured/orders/raw/")

print(f"Bronze records: {bronze_df.count()}")

# ETL Transformations
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

print(f"Silver records after transformation: {silver_df.count()}")

# Write to S3 Tables
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

silver_df.writeTo(f"{NAMESPACE}.{SILVER_TABLE}") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print(f"✓ ETL Complete: Data written to s3tables.{NAMESPACE}.{SILVER_TABLE}")

job.commit()
