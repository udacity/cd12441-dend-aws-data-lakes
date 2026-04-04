"""
Complete Iceberg Table Setup with S3 Tables
Creates S3 table bucket, namespace, and silver Iceberg table for Exercise 1
"""

from load_env import load_env
import boto3
import time

load_env()

# Initialize clients
athena = boto3.client('athena', region_name='us-east-1')
s3tables = boto3.client('s3tables', region_name='us-east-1')
sts = boto3.client('sts')

# Configuration
ACCOUNT_ID = sts.get_caller_identity()['Account']
TABLE_BUCKET_NAME = 'swiftshop-analytics-tables'
NAMESPACE = 'swiftshop'
SILVER_TABLE = 'silver_orders'
ATHENA_BUCKET_NAME = 's3://swiftshop-data-lake/athena-results/'

def run_athena_query(query, description, catalog='awsdatacatalog'):
    """Execute Athena query and wait for completion"""
    print(f"{description}...")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': NAMESPACE, 'Catalog': catalog},
        ResultConfiguration={'OutputLocation': ATHENA_BUCKET_NAME}
    )
    query_id = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state == 'SUCCEEDED':
        print(f"✓ {description} completed")
        return True
    else:
        reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        print(f"✗ {description} failed: {reason}")
        return False

def create_s3_table_bucket():
    """Create S3 table bucket and namespace"""
    try:
        resp = s3tables.create_table_bucket(name=TABLE_BUCKET_NAME)
        arn = resp['arn']
        print(f"✓ Created table bucket: {arn}")
    except s3tables.exceptions.ConflictException:
        arn = f"arn:aws:s3tables:us-east-1:{ACCOUNT_ID}:bucket/{TABLE_BUCKET_NAME}"
        print(f"  Table bucket already exists: {arn}")

    try:
        s3tables.create_namespace(tableBucketARN=arn, namespace=[NAMESPACE])
        print(f"✓ Created namespace: {NAMESPACE}")
    except s3tables.exceptions.ConflictException:
        print(f"  Namespace already exists: {NAMESPACE}")

    return arn

def create_silver_table():
    """Create empty silver_orders Iceberg table in S3 Tables for Exercise 2"""
    catalog = f"s3tablescatalog/{TABLE_BUCKET_NAME}"

    run_athena_query(
        f"DROP TABLE IF EXISTS {NAMESPACE}.{SILVER_TABLE}",
        "Dropping existing silver table",
        catalog=catalog
    )

    create_sql = f"""
    CREATE TABLE {NAMESPACE}.{SILVER_TABLE} (
        order_id STRING,
        user_id STRING,
        product_id STRING,
        order_value DOUBLE,
        order_date TIMESTAMP,
        status STRING,
        processed_at TIMESTAMP
    )
    TBLPROPERTIES ('table_type' = 'ICEBERG')
    """
    run_athena_query(create_sql, "Creating silver table in S3 Tables", catalog=catalog)

if __name__ == "__main__":
    print("=== Exercise 1: S3 Tables Iceberg Setup ===\n")
    create_s3_table_bucket()
    create_silver_table()
    print(f"\n=== Setup Complete ===")
    print(f"✓ Table bucket: {TABLE_BUCKET_NAME}")
    print(f"✓ Namespace: {NAMESPACE}")
    print(f"✓ Silver table: {SILVER_TABLE} (empty, for Exercise 1)")
    print(f"\nVerify in Athena (use catalog: s3tablescatalog/{TABLE_BUCKET_NAME}):")
    print(f"DESCRIBE {NAMESPACE}.{SILVER_TABLE};")
