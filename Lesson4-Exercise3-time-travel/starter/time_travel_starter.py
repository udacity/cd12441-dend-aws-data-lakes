"""
Exercise 3 Starter: Time Travel and Schema Evolution
Student Name: _______________
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

# TODO: Query current version
current_data = spark.sql(# YOUR CODE HERE)
print("Current version:")
current_data.show(5)

# TODO: Query historical version (version 1)
historical_data = spark.sql("""
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders 
FOR VERSION AS OF # YOUR CODE HERE
LIMIT 5
""")
print("Historical version:")
historical_data.show(5)

# TODO: Show table history
table_history = spark.sql(# YOUR CODE HERE)
print("Table history:")
table_history.show()

# TODO: Add new column
spark.sql("""
ALTER TABLE s3tables_catalog.cloudmart_db.bronze_orders 
ADD COLUMN # YOUR CODE HERE
""")

# TODO: Compact small files
spark.sql(# YOUR CODE HERE)

# TODO: Update statistics
spark.sql(# YOUR CODE HERE)

# TODO: Compare current vs historical
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
    # YOUR CODE HERE - calculate growth metrics
FROM current_stats, historical_stats
""")
print("Growth analysis:")
comparison.show()

job.commit()
