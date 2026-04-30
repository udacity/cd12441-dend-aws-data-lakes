"""
Exercise 3 Starter: Time Travel and Schema Evolution
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tables") \
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.s3tables.glue.id", "ACCOUNT:s3tablescatalog/swiftshop-analytics-tables") \
    .config("spark.sql.catalog.s3tables.warehouse", "s3://swiftshop-analytics-tables/bucket/swiftshop-analytics-tables") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TABLE = "swiftshop.silver_orders"

print("=== TIME TRAVEL AND SCHEMA EVOLUTION ===")

COLS = "order_id, user_id, product_id, order_value, order_date, status, processed_at"

# TODO 1: Query current version
current_data = spark.sql(# YOUR CODE HERE)
print("Current version:")
current_data.show()

# TODO 2: Show table history
table_history = spark.sql(# YOUR CODE HERE)
print("Table history:")
table_history.show()

# TODO 3: Query historical version using snapshot_id
# Hint: Get first snapshot_id from history, then use FOR VERSION AS OF
first_snapshot = # YOUR CODE HERE

historical_data = spark.sql(# YOUR CODE HERE)
print("Historical version:")
historical_data.show()

# TODO 4: Compare versions
# Calculate count and avg order_value for current vs historical
# YOUR CODE HERE

# TODO 5: Schema evolution - add column
# Hint: ALTER TABLE ... ADD COLUMN customer_segment STRING
# YOUR CODE HERE

# 6. S3 Tables handles file compaction automatically
print("✓ S3 Tables handles file compaction automatically")

job.commit()
