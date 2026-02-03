"""
Exercise 1 Starter: Iceberg Table Setup with S3 Tables
Student Name: _______________
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

# TODO: Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# TODO: Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== ICEBERG TABLE SETUP ===")

# TODO: Configure Spark for Iceberg
spark.conf.set("spark.sql.extensions", # YOUR CODE HERE)
spark.conf.set("spark.sql.catalog.s3tables_catalog", # YOUR CODE HERE)
spark.conf.set("spark.sql.catalog.s3tables_catalog.warehouse", # YOUR CODE HERE)

# TODO: Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# TODO: Create database
spark.sql(# YOUR CODE HERE)

# TODO: Create bronze_orders table with Iceberg
spark.sql("""
CREATE TABLE IF NOT EXISTS s3tables_catalog.cloudmart_db.bronze_orders (
    # YOUR CODE HERE - define columns
) USING iceberg 
PARTITIONED BY (days(order_date))
""")

print("Iceberg table created")

# TODO: Load data from S3
bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [# YOUR CODE HERE]}
).toDF()

# TODO: Add metadata columns
bronze_df = bronze_df.withColumn("ingestion_timestamp", # YOUR CODE HERE) \
                    .withColumn("data_source", # YOUR CODE HERE)

# TODO: Write to Iceberg table
bronze_df.write.format(# YOUR CODE HERE).mode(# YOUR CODE HERE).saveAsTable(# YOUR CODE HERE)

print(f"Loaded {bronze_df.count()} records")

job.commit()
