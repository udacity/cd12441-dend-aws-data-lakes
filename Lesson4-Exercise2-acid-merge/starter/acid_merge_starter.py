"""
Exercise 2 Starter: ACID Transactions and MERGE Operations
Student Name: _______________
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

# TODO: Create silver_orders table
spark.sql("""
CREATE TABLE IF NOT EXISTS s3tables_catalog.cloudmart_db.silver_orders (
    # YOUR CODE HERE - define columns
) USING iceberg 
PARTITIONED BY (days(order_date))
""")

# TODO: Read CDC updates from S3
updates_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [# YOUR CODE HERE]}
).toDF()

# TODO: Add metadata columns
updates_df = updates_df.withColumn("updated_at", # YOUR CODE HERE) \
                      .withColumn("status", # YOUR CODE HERE)

# TODO: Create temp view
updates_df.createOrReplaceTempView(# YOUR CODE HERE)

print(f"CDC updates: {updates_df.count()}")

# TODO: Execute MERGE statement
spark.sql("""
MERGE INTO s3tables_catalog.cloudmart_db.silver_orders AS target
USING cdc_updates AS source
ON # YOUR CODE HERE - match condition
WHEN MATCHED THEN UPDATE SET 
    # YOUR CODE HERE - update columns
WHEN NOT MATCHED THEN INSERT (
    # YOUR CODE HERE - insert columns
) VALUES (
    # YOUR CODE HERE - insert values
)
""")

print("MERGE completed")

# TODO: Verify results
result = spark.sql(# YOUR CODE HERE)
result.show()

job.commit()
