"""
Exercise 2 Starter: CDC with Bookmarks
Student Name: _______________
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import max as spark_max

# TODO: Get job arguments including bookmark
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bookmark'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# TODO: Build incremental query using bookmark
query = # YOUR CODE HERE (Hint: WHERE updated_at > bookmark)

# TODO: Create dynamic frame with incremental query
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": # YOUR CODE HERE,
        "dbtable": # YOUR CODE HERE (use query variable),
        "user": # YOUR CODE HERE,
        "password": # YOUR CODE HERE
    }
)

record_count = datasource.count()

if record_count > 0:
    df = datasource.toDF()
    
    # TODO: Get max updated_at for next bookmark
    max_updated_at = # YOUR CODE HERE
    
    # TODO: Write to S3 in append mode
    df.write.mode(# YOUR CODE HERE).parquet("s3://cloudmart/bronze/orders/")
    
    print(f"Next bookmark: {max_updated_at}")
else:
    print("No new records")

job.commit()
