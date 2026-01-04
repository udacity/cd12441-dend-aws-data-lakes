"""
Exercise 2 Starter Code: CDC Ingestion to Bronze Layer
Student Name: _______________
Date: _______________

Instructions:
1. Complete the TODO sections for CDC implementation
2. Deploy as AWS Glue ETL job with bookmark parameter
3. Test incremental loading by updating PostgreSQL data
4. Verify partitioned files in S3 and query with Athena
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp

# TODO: Get job arguments including JOB_NAME and bookmark
# Hint: Use getResolvedOptions with sys.argv
args = # YOUR CODE HERE

# TODO: Initialize Spark and Glue contexts
sc = # YOUR CODE HERE
glueContext = # YOUR CODE HERE
spark = # YOUR CODE HERE

# TODO: Initialize Glue job
job = # YOUR CODE HERE
# YOUR CODE HERE - job.init()

print(f"Starting CDC ingestion job: {args['JOB_NAME']}")
print(f"Using bookmark: {args['bookmark']}")

# TODO: Build incremental query using bookmark parameter
# Create a subquery that selects records where updated_at > bookmark
query = # YOUR CODE HERE (Hint: f"(SELECT * FROM orders WHERE updated_at > '{args['bookmark']}') as incremental_query")

print(f"Executing query: {query}")

# TODO: Create dynamic frame from PostgreSQL using JDBC
# Configure connection options for PostgreSQL RDS
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        # TODO: Fill in connection parameters
        "url": # YOUR CODE HERE,
        "dbtable": # YOUR CODE HERE,
        "user": # YOUR CODE HERE,
        "password": # YOUR CODE HERE,
        "customJdbcDriverS3Path": # YOUR CODE HERE (optional),
        "customJdbcDriverClassName": # YOUR CODE HERE (optional)
    }
)

print(f"Records retrieved: {datasource.count()}")

# TODO: Convert to DataFrame and add partitioning column
df = # YOUR CODE HERE
# TODO: Add order_date column by casting updated_at to date
df = df.withColumn("order_date", # YOUR CODE HERE)

# TODO: Show sample data for verification
print("Sample data:")
# YOUR CODE HERE

# TODO: Write to S3 bronze layer with partitioning
# Use coalesce to optimize file sizes, append mode, partition by order_date
df.coalesce(# YOUR CODE HERE) \
  .write \
  .mode(# YOUR CODE HERE) \
  .partitionBy(# YOUR CODE HERE) \
  .parquet(# YOUR CODE HERE)

print("Data written to S3 bronze layer")

# TODO: Commit the job to update bookmark
# YOUR CODE HERE

print("CDC ingestion job completed successfully")

# TODO: Print summary statistics
print(f"Records processed: {# YOUR CODE HERE}")
print(f"Target location: s3://cloudmart/bronze/orders/")
print(f"Next bookmark: {# YOUR CODE HERE}")

"""
Testing Instructions:
1. Deploy this job with initial bookmark: '1900-01-01 00:00:00'
2. Run job to load historical data
3. Update some records in PostgreSQL orders table
4. Run job again with new bookmark (max updated_at from previous run)
5. Verify incremental files created in S3
6. Query with Athena: SELECT count(*) FROM bronze_orders WHERE order_date = '2024-01-01'
"""
