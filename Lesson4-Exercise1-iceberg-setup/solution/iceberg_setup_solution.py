"""
Exercise 1 Solution: Iceberg Table Setup with S3 Tables
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

print("=== ICEBERG TABLE SETUP ===")

# Configure Iceberg
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.s3tables_catalog.warehouse", "s3://cloudmart-s3tables-metadata/warehouse")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS s3tables_catalog.cloudmart_db")

# Create Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS s3tables_catalog.cloudmart_db.bronze_orders (
    order_id bigint,
    user_id string,
    product_id string,
    order_value double,
    order_date timestamp,
    ingestion_timestamp timestamp,
    data_source string
) USING iceberg 
PARTITIONED BY (days(order_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
""")

print("Iceberg table created")

# Load data
bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://cloudmart/bronze/orders/"]}
).toDF()

bronze_df = bronze_df.withColumn("ingestion_timestamp", current_timestamp()) \
                    .withColumn("data_source", lit("s3_bronze_layer"))

bronze_df.write.format("iceberg").mode("append").saveAsTable("s3tables_catalog.cloudmart_db.bronze_orders")

print(f"Loaded {bronze_df.count()} records to Iceberg table")

job.commit()
