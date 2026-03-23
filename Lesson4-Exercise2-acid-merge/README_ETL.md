# Exercise 2: Bronze to Silver ETL with AWS Glue

## Quick Start (Manual - Recommended for Lab Environments)

### 1. Upload Script
```bash
aws s3 cp solution/bronze_to_silver_etl.py s3://swiftshop-data-lake/glue-scripts/
```

### 2. Create Job in Console
1. Go to AWS Glue Console → ETL Jobs → Visual ETL
2. Click "Script editor" → "Create new job"
3. Paste contents of `bronze_to_silver_etl.py`
4. Job details:
   - Name: `bronze-to-silver-etl`
   - IAM Role: Select available Glue service role
   - Glue version: 4.0
   - Worker type: G.1X
   - Number of workers: 2
5. Job parameters → Add:
   - `--datalake-formats`: `iceberg`
   - `--enable-glue-datacatalog`: `true`
6. Save and Run

### 3. Monitor Job
- View run status in Glue Console
- Check CloudWatch logs for details

## Automated Deployment (If Role Permissions Allow)

```bash
python solution/deploy_and_run.py
```

Note: Requires IAM role with Glue service trust relationship.

## Verify Results

Query in Athena:
```sql
-- Check silver table
SELECT COUNT(*) FROM swiftshop.silver_orders;

-- Compare bronze vs silver
SELECT 
  (SELECT COUNT(*) FROM swiftshop.bronze_orders) as bronze_count,
  (SELECT COUNT(*) FROM swiftshop.silver_orders) as silver_count;

-- View cleaned data
SELECT * FROM swiftshop.silver_orders LIMIT 10;
```

## Expected Outcomes
- ✅ Null order_values filtered out
- ✅ Negative values cleaned to 0
- ✅ Null status standardized to "unknown"
- ✅ processed_at timestamp added
- ✅ Data written to Iceberg silver table
