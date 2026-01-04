"""
Exercise 2 Solution: CDC Ingestion to Bronze Layer
Complete AWS Glue ETL job for incremental data loading from PostgreSQL to S3
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, max as spark_max

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bookmark'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting CDC ingestion job: {args['JOB_NAME']}")
print(f"Using bookmark: {args['bookmark']}")

# Build incremental query using bookmark
query = f"(SELECT * FROM orders WHERE updated_at > '{args['bookmark']}') as incremental_query"

print(f"Executing query: {query}")

# Create dynamic frame from PostgreSQL
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://cloudmart-rds.cluster-xyz.us-east-1.rds.amazonaws.com:5432/cloudmart",
        "dbtable": query,
        "user": "cloudmart_user",
        "password": "cloudmart_password",
        "customJdbcDriverS3Path": "s3://aws-glue-assets/postgresql-42.2.16.jar",
        "customJdbcDriverClassName": "org.postgresql.Driver"
    }
)

record_count = datasource.count()
print(f"Records retrieved: {record_count}")

if record_count > 0:
    # Convert to DataFrame and add partitioning column
    df = datasource.toDF()
    
    # Add order_date column for partitioning
    df = df.withColumn("order_date", col("updated_at").cast("date"))
    
    # Show sample data
    print("Sample data:")
    df.show(5, truncate=False)
    
    # Get max updated_at for next bookmark
    max_updated_at = df.agg(spark_max("updated_at")).collect()[0][0]
    
    # Write to S3 bronze layer with partitioning
    df.coalesce(4) \
      .write \
      .mode("append") \
      .partitionBy("order_date") \
      .parquet("s3://cloudmart/bronze/orders/")
    
    print("Data written to S3 bronze layer")
    
    # Print summary statistics
    print(f"Records processed: {record_count}")
    print(f"Target location: s3://cloudmart/bronze/orders/")
    print(f"Next bookmark: {max_updated_at}")
    
else:
    print("No new records found since last bookmark")

# Commit job
job.commit()

print("CDC ingestion job completed successfully")

"""
Deployment Instructions:

1. Create Glue Job:
aws glue create-job \
  --name "cloudmart-cdc-ingestion" \
  --role "AWSGlueServiceRole" \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-scripts-bucket/cdc_ingestion_solution.py"
  }' \
  --default-arguments '{
    "--bookmark": "1900-01-01 00:00:00"
  }'

2. Run Job:
aws glue start-job-run \
  --job-name "cloudmart-cdc-ingestion" \
  --arguments '--bookmark=2024-01-01 00:00:00'

3. Verify Results with Athena:
CREATE EXTERNAL TABLE bronze_orders (
  order_id bigint,
  user_id string,
  order_value double,
  updated_at timestamp
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://cloudmart/bronze/orders/'

MSCK REPAIR TABLE bronze_orders;
SELECT count(*) FROM bronze_orders WHERE order_date = '2024-01-01';

4. Test CDC:
-- Update PostgreSQL
UPDATE orders SET order_value = 150.00, updated_at = NOW() WHERE order_id = 123;

-- Run Glue job again with new bookmark
-- Verify new partition created in S3
"""
