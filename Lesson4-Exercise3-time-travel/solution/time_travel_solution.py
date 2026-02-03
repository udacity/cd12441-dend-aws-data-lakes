"""
Exercise 3 Solution: Time Travel and Schema Evolution
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== TIME TRAVEL AND SCHEMA EVOLUTION ===")

spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Query current version
current_data = spark.sql("SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders LIMIT 5")
print("Current version:")
current_data.show()

# Query historical version
historical_data = spark.sql("""
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders 
FOR VERSION AS OF 1 
LIMIT 5
""")
print("Historical version (v1):")
historical_data.show()

# Show table history
table_history = spark.sql("""
SELECT snapshot_id, committed_at, summary 
FROM s3tables_catalog.cloudmart_db.bronze_orders.history 
ORDER BY committed_at DESC
""")
print("Table history:")
table_history.show()

# Schema evolution - add column
spark.sql("""
ALTER TABLE s3tables_catalog.cloudmart_db.bronze_orders 
ADD COLUMN customer_segment string
""")
print("Column added")

# Compact small files
spark.sql("CALL s3tables_catalog.system.rewrite_data_files('cloudmart_db.bronze_orders')")
print("Files compacted")

# Update statistics
spark.sql("ANALYZE TABLE s3tables_catalog.cloudmart_db.bronze_orders COMPUTE STATISTICS")
print("Statistics updated")

# Compare versions
comparison = spark.sql("""
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
print("Growth analysis:")
comparison.show()

job.commit()
