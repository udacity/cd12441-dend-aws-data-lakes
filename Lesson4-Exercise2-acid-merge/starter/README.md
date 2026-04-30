# Exercise 2: Bronze to Silver ETL with ACID Transactions - Student Instructions

## Objective
Implement a Bronze to Silver ETL pipeline using PySpark on AWS Glue with Iceberg tables in S3 Tables.

## What You'll Learn
- Read bronze Parquet data from S3
- Apply ETL transformations (null handling, data cleaning, column renaming)
- Write to Iceberg tables in S3 Tables
- Deploy and run Glue jobs programmatically
- Grant Lake Formation permissions for S3 Tables access

## Prerequisites
- AWS credentials in `/workspace/.env` (see the course **Setup** page; re-paste from the Cloud Resources tab if your session token has expired)
- Exercise 1 completed (S3 table bucket and silver_orders table created)
- Bronze data available in S3 (from Lesson 1)
- Deploy the Glue IAM role:
  ```bash
  aws cloudformation deploy --template-file glue-role.yaml --stack-name glue-etl-role --capabilities CAPABILITY_NAMED_IAM
  ```

## Starter Files

| File | Purpose |
|------|---------|
| `bronze_to_silver_etl.py` | PySpark ETL script (runs on Glue) |
| `deploy_and_run.py` | Uploads script to S3, creates and runs the Glue job |
| `grant_lakeformation.py` | Grants Lake Formation permissions to the Glue role |

## Step-by-Step Instructions

### Step 1: Grant Lake Formation Permissions
```bash
python grant_lakeformation.py
```
Complete the TODOs to grant catalog, database, and table-level permissions.

### Step 2: Implement the ETL Script (`bronze_to_silver_etl.py`)

#### TODO 1: Read Bronze Data
```python
bronze_df = spark.read.format("parquet") \
    .load("s3://YOUR-BRONZE-BUCKET/structured/orders/raw/")
```

#### TODO 2: Apply ETL Transformations
- Filter null `order_value`
- Clean negative values to 0
- Replace null `status` with "unknown"
- Add `processed_at` timestamp
- Select and rename columns

#### TODO 3: Write to S3 Tables
```python
silver_df.writeTo(f"{NAMESPACE}.{SILVER_TABLE}") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
```

### Step 3: Deploy and Run (`deploy_and_run.py`)
```bash
python deploy_and_run.py
```
Complete the TODOs to upload the script, create the Glue job, and run it.

## Expected Output
```
=== Starting Bronze to Silver ETL ===
Bronze records: 10,000
Silver records after transformation: 9,480
✓ ETL Complete: Data written to s3tables.swiftshop.silver_orders
```

## Verification
Query in Athena (catalog: `s3tablescatalog/swiftshop-analytics-tables`):
```sql
SELECT COUNT(*) FROM swiftshop.silver_orders;
SELECT status, COUNT(*) FROM swiftshop.silver_orders GROUP BY status;
```

## Common Issues
- **Catalog not found**: Verify glue.id matches your account
- **S3 path error**: Check bronze bucket path
- **Access denied**: Run `grant_lakeformation.py` first
- **Schema mismatch**: Ensure column names match table definition

## Success Criteria
✅ Lake Formation permissions granted
✅ Bronze data read from S3
✅ Null values filtered and negative values cleaned
✅ Data written to Iceberg table in S3 Tables
✅ Glue job deployed and executed successfully
