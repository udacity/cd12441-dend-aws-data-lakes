"""
Exercise 1 Starter: JDBC Connection and Full Table Load
Student Name: _______________
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# TODO: Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# TODO: Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# TODO: Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# TODO: Create dynamic frame from PostgreSQL
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": # YOUR CODE HERE,
        "dbtable": # YOUR CODE HERE,
        "user": # YOUR CODE HERE,
        "password": # YOUR CODE HERE
    }
)

print(f"Records retrieved: {datasource.count()}")

# TODO: Convert to DataFrame and show sample
df = datasource.toDF()
df.show(5)

# TODO: Write to S3 bronze layer
df.write.mode("overwrite").parquet("s3://cloudmart/bronze/orders/")

job.commit()
