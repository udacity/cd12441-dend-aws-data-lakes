"""
Exercise 3 Solution: Partitioning and Optimization
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, max as spark_max

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bookmark'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting optimized CDC with partitioning")

query = f"(SELECT * FROM orders WHERE updated_at > '{args['bookmark']}') as incremental_query"

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://cloudmart-rds.cluster-xyz.us-east-1.rds.amazonaws.com:5432/cloudmart",
        "dbtable": query,
        "user": "cloudmart_user",
        "password": "cloudmart_password"
    }
)

record_count = datasource.count()

if record_count > 0:
    df = datasource.toDF()
    
    # Add partitioning column
    df = df.withColumn("order_date", col("updated_at").cast("date"))
    
    max_updated_at = df.agg(spark_max("updated_at")).collect()[0][0]
    
    # Optimize file sizes with coalesce and partition by date
    df.coalesce(4) \
      .write \
      .mode("append") \
      .partitionBy("order_date") \
      .parquet("s3://cloudmart/bronze/orders/")
    
    print(f"Records processed: {record_count}")
    print(f"Partitioned by order_date")
    print(f"Next bookmark: {max_updated_at}")
else:
    print("No new records")

job.commit()
