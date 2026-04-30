# Exercise 1: Create S3 Tables with Iceberg - Setup

## Objective
Create an S3 table bucket, namespace, and an empty Iceberg table using the S3 Tables API and Athena.

## What You'll Learn
- Create S3 table buckets with the `s3tables` API
- Create namespaces for organizing Iceberg tables
- Use Athena to create Iceberg tables with `TBLPROPERTIES`
- Handle idempotent resource creation with `ConflictException`

## Prerequisites
- AWS credentials in `/workspace/.env` (see the course **Setup** page; re-paste from the Cloud Resources tab if your session token has expired)
- An S3 bucket for Athena query results

## Step-by-Step Instructions

### TODO 1: Create S3 Table Bucket and Namespace
```python
resp = s3tables.create_table_bucket(name=TABLE_BUCKET_NAME)
arn = resp['arn']

s3tables.create_namespace(tableBucketARN=arn, namespace=[NAMESPACE])
```
Handle `ConflictException` for both calls if resources already exist.

### TODO 2: Create the Iceberg Table
Write a `CREATE TABLE` SQL statement with the schema and Iceberg properties:
```sql
CREATE TABLE swiftshop.silver_orders (
    order_id STRING,
    user_id STRING,
    product_id STRING,
    order_value DOUBLE,
    order_date TIMESTAMP,
    status STRING,
    processed_at TIMESTAMP
)
TBLPROPERTIES ('table_type' = 'ICEBERG')
```

## Running Your Code
```bash
python create_table.py
```

## Verification
Query in Athena (use catalog: `s3tablescatalog/swiftshop-analytics-tables`):
```sql
DESCRIBE swiftshop.silver_orders;
```

## Expected Output
```
=== Exercise 1: S3 Tables Iceberg Setup ===

✓ Created table bucket: arn:aws:s3tables:us-east-1:ACCOUNT_ID:bucket/swiftshop-analytics-tables
✓ Created namespace: swiftshop
✓ Creating silver table in S3 Tables completed

=== Setup Complete ===
✓ Table bucket: swiftshop-analytics-tables
✓ Namespace: swiftshop
✓ Silver table: silver_orders (empty, for Exercise 2)
```

## Common Issues
- **ConflictException**: Resource already exists — this is safe to ignore
- **Access denied on Athena**: Check that `ATHENA_BUCKET_NAME` points to a valid S3 location you own
- **Catalog not found**: Use `s3tablescatalog/swiftshop-analytics-tables` as the catalog

## Success Criteria
✅ Table bucket created
✅ Namespace created
✅ silver_orders Iceberg table created (empty, ready for Exercise 2)
