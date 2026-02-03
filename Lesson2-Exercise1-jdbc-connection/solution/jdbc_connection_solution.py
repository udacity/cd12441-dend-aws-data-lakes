"""
Exercise 1 Solution: JDBC Connection and Full Table Load
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Starting full table load from PostgreSQL")

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://cloudmart-rds.cluster-xyz.us-east-1.rds.amazonaws.com:5432/cloudmart",
        "dbtable": "orders",
        "user": "cloudmart_user",
        "password": "cloudmart_password"
    }
)

record_count = datasource.count()
print(f"Records retrieved: {record_count}")

df = datasource.toDF()
print("Sample data:")
df.show(5, truncate=False)

df.write.mode("overwrite").parquet("s3://cloudmart/bronze/orders/")

print(f"Full table load completed: {record_count} records written to S3")

job.commit()
