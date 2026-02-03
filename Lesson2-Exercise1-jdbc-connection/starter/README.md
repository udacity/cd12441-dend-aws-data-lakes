# Exercise 1: JDBC Connection and Full Table Load - Student Instructions

## Objective
Connect AWS Glue to PostgreSQL RDS and load the complete orders table to S3 bronze layer.

## What You'll Learn
- Configure JDBC connections in AWS Glue
- Use GlueContext to read from relational databases
- Write data to S3 in Parquet format
- Understand full table load patterns

## Prerequisites
- Pre-configured AWS Glue environment in controlled AWS account
- PostgreSQL RDS instance pre-provisioned with CloudMart database
- S3 bucket already created: `s3://cloudmart/bronze/orders/`
- JDBC driver pre-configured in Glue environment

## Step-by-Step Instructions

### Step 1: Review the Starter Code
Open `jdbc_connection_starter.py` and identify all `# YOUR CODE HERE` sections.

### Step 2: Configure JDBC Connection
Complete the `connection_options` dictionary:
- **url**: JDBC connection string format: `jdbc:postgresql://HOST:PORT/DATABASE`
- **dbtable**: Table name to read (use `"orders"`)
- **user**: Database username
- **password**: Database password

### Step 3: Convert and Display Data
- Convert the dynamic frame to a DataFrame using `.toDF()`
- Use `.show(5)` to display sample records

### Step 4: Write to S3
- Use `.write.mode("overwrite")` for full table load
- Write as Parquet format to `s3://cloudmart/bronze/orders/`

### Step 5: Test Your Solution
Run the Glue job and verify:
```bash
aws glue start-job-run --job-name jdbc-connection-exercise
```

Check S3 for Parquet files:
```bash
aws s3 ls s3://cloudmart/bronze/orders/
```

## Expected Output
```
Records retrieved: 1000
[Sample data displayed]
Full table load completed: 1000 records written to S3
```

## Common Issues
- **Connection timeout**: Check security groups and VPC configuration
- **Authentication failed**: Verify username/password
- **S3 access denied**: Check IAM role permissions

## Verification with Athena
```sql
CREATE EXTERNAL TABLE bronze_orders (
  order_id bigint,
  user_id string,
  order_value double,
  updated_at timestamp
)
STORED AS PARQUET
LOCATION 's3://cloudmart/bronze/orders/';

SELECT COUNT(*) FROM bronze_orders;
```

## Success Criteria
✅ Glue job runs without errors  
✅ Parquet files created in S3  
✅ Data queryable in Athena  
✅ Record count matches source table
