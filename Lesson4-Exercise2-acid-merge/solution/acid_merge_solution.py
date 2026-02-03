"""
Exercise 2 Solution: ACID Transactions and MERGE Operations
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== ACID TRANSACTIONS AND MERGE ===")

spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create silver table
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

# Read CDC updates
updates_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://cloudmart/cdc/updates/"]}
).toDF()

updates_df = updates_df.withColumn("updated_at", current_timestamp()) \
                      .withColumn("status", lit("updated"))

updates_df.createOrReplaceTempView("cdc_updates")

print(f"CDC updates to process: {updates_df.count()}")

# Execute MERGE
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

print("MERGE operation completed")

# Verify results
result = spark.sql("SELECT status, COUNT(*) as count FROM s3tables_catalog.cloudmart_db.silver_orders GROUP BY status")
result.show()

job.commit()
