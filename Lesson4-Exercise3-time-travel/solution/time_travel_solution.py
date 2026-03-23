"""
Exercise 3 Solution: Time Travel and Schema Evolution
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
    .config("spark.sql.catalog.s3tables.glue.id", "633948902050:s3tablescatalog/swiftshop-analytics-tables") \
    .config("spark.sql.catalog.s3tables.warehouse", "s3://swiftshop-analytics-tables/bucket/swiftshop-analytics-tables") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TABLE = "swiftshop.silver_orders"

print("=== TIME TRAVEL AND SCHEMA EVOLUTION ===")

COLS = "order_id, user_id, product_id, order_value, order_date, status, processed_at"

# 1. Query current version
current_data = spark.sql(f"SELECT {COLS} FROM {TABLE} LIMIT 5")
print("Current version:")
current_data.show()

# 2. Show table history
table_history = spark.sql(f"""
SELECT snapshot_id, made_current_at, is_current_ancestor 
FROM {TABLE}.history 
ORDER BY made_current_at DESC
""")
print("Table history:")
table_history.show()

# 3. Query historical version
first_snapshot = spark.sql(f"""
SELECT snapshot_id FROM {TABLE}.history 
ORDER BY made_current_at ASC LIMIT 1
""").collect()[0][0]

historical_data = spark.sql(f"""
SELECT {COLS} FROM {TABLE} 
FOR VERSION AS OF {first_snapshot} 
LIMIT 5
""")
print("Historical version:")
historical_data.show()

# 4. Compare versions (before schema evolution)
current_stats = spark.sql(f"""
    SELECT COUNT(*) as cnt, AVG(order_value) as avg_val FROM {TABLE}
""").collect()[0]

historical_stats = spark.sql(f"""
    SELECT COUNT(*) as cnt, AVG(order_value) as avg_val 
    FROM {TABLE} FOR VERSION AS OF {first_snapshot}
""").collect()[0]

print("Growth analysis:")
print(f"  Current count: {current_stats['cnt']}, Historical count: {historical_stats['cnt']}, Growth: {current_stats['cnt'] - historical_stats['cnt']}")
print(f"  Current avg: {current_stats['avg_val']:.2f}, Historical avg: {historical_stats['avg_val']:.2f}, Change: {current_stats['avg_val'] - historical_stats['avg_val']:.2f}")

# 5. Schema evolution - add column
try:
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN customer_segment STRING")
    print("✓ Column 'customer_segment' added")
except Exception as e:
    print(f"  Column already exists, skipping")

# 6. S3 Tables handles file compaction automatically
print("✓ S3 Tables handles file compaction automatically")

job.commit()
