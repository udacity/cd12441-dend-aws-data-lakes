# Lesson 2 - Exercise 3: AWS Glue Crawler and Data Catalog

## Overview
This exercise demonstrates AWS Glue Data Catalog - the centralized metadata repository for your data lake. You'll create a Glue crawler that automatically discovers schemas and partitions from S3 data, making it queryable by Athena, EMR, and Redshift Spectrum.

## Learning Objectives
- ✅ Understand AWS Glue Data Catalog as metadata repository
- ✅ Create and configure Glue crawlers
- ✅ Automatic schema inference from Parquet files
- ✅ Automatic partition discovery
- ✅ Query cataloged data with Athena
- ✅ Handle schema evolution

## Prerequisites
- Lesson 2 Exercise 2 completed (DMS data in S3)
- AWS Glue permissions configured
- **IAM role for Glue crawler** (see setup below)
- Environment variables:
  - `export BUCKET_NAME="your-bucket-name"`
  - `export GLUE_ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT:role/GlueServiceRole"`
- Python with boto3 installed (no Spark required)

### IAM Role Setup (Required)

Create a Glue service role:
```bash
# Create role
aws iam create-role \
  --role-name GlueServiceRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "glue.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach Glue service policy
aws iam attach-role-policy \
  --role-name GlueServiceRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Attach S3 read policy
aws iam attach-role-policy \
  --role-name GlueServiceRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

# Get role ARN
aws iam get-role --role-name GlueServiceRole --query 'Role.Arn' --output text
```

Set environment variable:
```bash
export GLUE_ROLE_ARN="arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueServiceRole"
```

## Concepts Covered

### AWS Glue Data Catalog

**Definition**: Centralized metadata repository that stores table schemas, locations, and partitions. Acts as a Hive-compatible metastore.

**Key Components**:
- **Database**: Logical grouping of tables
- **Table**: Metadata about dataset (schema, location, format)
- **Partition**: Subset of data organized by key (e.g., date)
- **Crawler**: Automated tool that discovers and catalogs data

### Glue Crawler Workflow

```
1. Scan S3 Data
   ↓
2. Infer Schema (from Parquet metadata)
   ↓
3. Register Table (in Glue Catalog)
   ↓
4. Discover Partitions (from folder structure)
```

### Schema Inference

Crawler reads Parquet file metadata and automatically detects:

```python
# Parquet metadata → Glue schema
order_id: int64        → bigint
total_amount: float64  → double
status: string         → string
order_date: date       → date
```

**No DDL required** - completely automatic!

### Partition Discovery

Crawler recognizes folder patterns:

```
s3://bucket/orders/
├── order_date=2026-01-03/
│   └── data.parquet
├── order_date=2026-01-04/
│   └── data.parquet
└── order_date=2026-01-05/
    └── data.parquet
```

Automatically registers 3 partitions with key `order_date`.

### Glue Catalog Benefits

1. **Single Source of Truth**: One metadata repository for all tools
2. **No Manual DDL**: Schema inference eliminates manual table creation
3. **Seamless Integration**: Works with Athena, EMR, Redshift Spectrum
4. **Schema Evolution**: Crawler detects and updates schema changes
5. **Partition Management**: Automatic partition discovery and updates

## Running the Exercise

### Step 1: Run Exercise
```bash
cd /Users/emota/Projects/cd12441-dend-aws-data-lakes/Lesson2-Exercise3-glue-catalog/starter
export BUCKET_NAME="your-bucket-name"
python glue_catalog_starter.py
```

### Expected Output
```
======================================================================
LESSON 2 - EXERCISE 3: AWS GLUE CRAWLER AND DATA CATALOG
======================================================================

[Step 1] Creating Glue Database...
  Database: swiftshop_catalog
  Description: Metadata repository for SwiftShop data lake
✓ Database created

[Step 2] Preparing Sample Data in S3...
  Uploading partitioned orders data...
  s3://bucket/swiftshop/orders/order_date=2026-01-03/
  s3://bucket/swiftshop/orders/order_date=2026-01-04/
  s3://bucket/swiftshop/orders/order_date=2026-01-05/
✓ Sample data uploaded (3 partitions, 150 orders)

[Step 3] Creating Glue Crawler...
  Crawler name: swiftshop-orders-crawler
  Target: s3://bucket/swiftshop/orders/
  Database: swiftshop_catalog
  Schedule: None (manual trigger)
  
  Crawler Configuration:
  • Schema inference: Enabled (from Parquet metadata)
  • Partition discovery: Enabled (from folder structure)
  • Schema evolution: Update table on changes
  
✓ Crawler created

[Step 4] Running Crawler...
  Starting crawler: swiftshop-orders-crawler
  Status: RUNNING
  
  Crawler Progress:
  [00:15] Scanning S3 paths...
  [00:30] Reading Parquet metadata...
  [00:45] Inferring schema...
  [01:00] Discovering partitions...
  [01:15] Registering table...
  
  Status: SUCCEEDED
  Duration: 1m 23s
  
  Crawler Results:
  • Tables created: 1
  • Partitions discovered: 3
  • Rows scanned: 150

[Step 5] Examining Cataloged Table...

  📊 Table: orders
  
  Schema (inferred from Parquet):
  ┌─────────────────┬──────────┬─────────────┐
  │ Column          │ Type     │ Source      │
  ├─────────────────┼──────────┼─────────────┤
  │ order_id        │ bigint   │ Parquet     │
  │ user_id         │ string   │ Parquet     │
  │ product_id      │ string   │ Parquet     │
  │ order_value     │ double   │ Parquet     │
  │ status          │ string   │ Parquet     │
  │ created_at      │ timestamp│ Parquet     │
  └─────────────────┴──────────┴─────────────┘
  
  Partition Key: order_date (string)
  
  Storage:
  • Location: s3://bucket/swiftshop/orders/
  • Format: Parquet
  • Compression: Snappy
  
  Partitions Discovered:
  • order_date=2026-01-03 (50 rows)
  • order_date=2026-01-04 (50 rows)
  • order_date=2026-01-05 (50 rows)

[Step 6] Querying Cataloged Data with Athena...

  Query 1: Count all orders
  SELECT COUNT(*) FROM swiftshop_catalog.orders
  
  Result: 150 orders
  Execution time: 1.2s
  Data scanned: 0.015 MB

  Query 2: Query specific partition
  SELECT * FROM swiftshop_catalog.orders 
  WHERE order_date = '2026-01-03'
  
  Result: 50 orders
  Execution time: 0.8s
  Data scanned: 0.005 MB (partition pruning!)

[Step 7] Demonstrating Schema Evolution...

  Scenario: New column added to source data
  
  Original Schema:
  • order_id, user_id, product_id, order_value, status, created_at
  
  Adding new partition with extra column:
  • order_date=2026-01-06/
  • New column: shipping_address (string)
  
  Running crawler again...
  Status: RUNNING
  [00:30] Detecting schema changes...
  [00:45] Updating table schema...
  Status: SUCCEEDED
  
  Updated Schema:
  • order_id, user_id, product_id, order_value, status, created_at
  • shipping_address (string) ← NEW COLUMN
  
  ✓ Schema evolution handled automatically!
  ✓ Old partitions: shipping_address = NULL
  ✓ New partitions: shipping_address populated

[Step 8] Crawler Scheduling Options...

  Manual Trigger:
  • Run on-demand via console or API
  • Use case: Ad-hoc data loads
  
  Scheduled:
  • Cron expression: "cron(0 * * * ? *)" (hourly)
  • Use case: Regular batch loads
  
  Event-Driven:
  • S3 event → Lambda → Start Crawler
  • Use case: Real-time data arrival
  
  For SwiftShop:
  • DMS writes data every 15 minutes
  • Crawler runs hourly to discover new partitions
  • Athena queries always see latest data

[Step 9] Glue Catalog Integration...

  The cataloged table is now queryable by:
  
  ✓ Amazon Athena
    SELECT * FROM swiftshop_catalog.orders
  
  ✓ Amazon EMR (Spark)
    spark.table("swiftshop_catalog.orders")
  
  ✓ Amazon Redshift Spectrum
    SELECT * FROM spectrum.swiftshop_catalog.orders
  
  ✓ AWS Glue ETL Jobs
    glueContext.create_dynamic_frame.from_catalog(
        database="swiftshop_catalog",
        table_name="orders"
    )
  
  Single metadata definition → Multiple query engines!

[Step 10] Best Practices...

  ✅ 1. Partition Strategy
     • Use date-based partitions for time-series data
     • Target 128 MB - 1 GB per partition
     • Avoid over-partitioning (too many small files)
  
  ✅ 2. Crawler Configuration
     • Enable schema evolution for changing data
     • Set appropriate schedule (hourly for batch, on-demand for real-time)
     • Use IAM role with minimal S3 permissions
  
  ✅ 3. Schema Management
     • Let crawler handle schema inference (don't manually define)
     • Monitor schema changes via CloudWatch
     • Test queries after schema evolution
  
  ✅ 4. Cost Optimization
     • Crawler charges: $0.44 per DPU-hour
     • Typical run: 1-2 minutes = $0.01-0.02
     • Schedule wisely to avoid unnecessary runs

======================================================================
EXERCISE 3 SUMMARY
======================================================================

📊 Glue Catalog Results:
   • Database: swiftshop_catalog
   • Table: orders
   • Schema: 6 columns (auto-inferred)
   • Partitions: 3 discovered
   • Crawler runtime: 1m 23s

✅ Key Learnings:
   1. Glue Crawler automates schema discovery
   2. No manual DDL required - reads Parquet metadata
   3. Automatic partition discovery from folder structure
   4. Schema evolution handled automatically
   5. Single catalog → Multiple query engines

🎯 Real-World Benefits:
   • "Set it and forget it" - crawler handles everything
   • Schema changes don't break queries
   • Consistent metadata across Athena, EMR, Redshift
   • Reduced operational overhead

💰 Cost Analysis:
   • Crawler: $0.02 per run (hourly = $14.40/month)
   • Catalog storage: First 1M objects free
   • Athena queries: $5/TB scanned (unchanged)
   • Total: ~$15/month for automated metadata management

======================================================================
Next: Lesson 3 - Bronze to Silver Transformation
======================================================================
```

## What You Learned

### 1. Glue Crawler Automation
- **Scans S3**: Reads data files automatically
- **Infers Schema**: Extracts from Parquet metadata
- **Discovers Partitions**: Recognizes folder patterns
- **Registers Table**: Creates queryable metadata

### 2. Schema Inference
```
Parquet File Metadata → Glue Schema
order_id: int64       → bigint
order_value: float64  → double
status: string        → string
```

### 3. Partition Discovery
```
Folder Structure:
s3://bucket/orders/order_date=2026-01-03/

Glue Catalog:
Partition Key: order_date
Partition Value: 2026-01-03
```

### 4. Schema Evolution
- Crawler detects new columns
- Updates table schema automatically
- Old partitions: NULL for new columns
- New partitions: Values populated

## Verification

Check Glue Catalog:
```bash
# List databases
aws glue get-databases

# Get table details
aws glue get-table --database-name swiftshop_catalog --name orders

# List partitions
aws glue get-partitions --database-name swiftshop_catalog --table-name orders
```

Query with Athena:
```sql
SELECT * FROM swiftshop_catalog.orders LIMIT 10;
```

## Common Issues

| Issue | Solution |
|-------|----------|
| `Crawler fails` | Check IAM role has S3 read permissions |
| `No partitions found` | Verify folder structure uses key=value format |
| `Schema not updated` | Enable "Update table" in crawler config |
| `Access denied` | Add Glue permissions to IAM role |
| `Table not found` | Wait for crawler to complete, check status in console |
| `IAM role error` | Set GLUE_ROLE_ARN environment variable |

### Detailed Troubleshooting

**Crawler Fails:**
```bash
# Check crawler status
aws glue get-crawler --name swiftshop-orders-crawler

# View crawler logs in CloudWatch
aws logs tail /aws-glue/crawlers --follow
```

**IAM Role Issues:**
```bash
# Verify role exists
aws iam get-role --role-name GlueServiceRole

# Check attached policies
aws iam list-attached-role-policies --role-name GlueServiceRole
```

**Table Not Found:**
- Crawler may have failed - check AWS Glue console
- Data may not exist in S3 - verify with `aws s3 ls`
- Wait longer for crawler to complete (can take 2-5 minutes)

## Discussion Questions

1. **Why use Glue Catalog instead of manual DDL?**
   - Automation: No manual schema definition
   - Evolution: Handles schema changes automatically
   - Integration: Works across multiple tools

2. **When should crawler run?**
   - **Hourly**: Regular batch loads
   - **On-demand**: Ad-hoc data arrival
   - **Event-driven**: Real-time S3 uploads

3. **What's the cost trade-off?**
   - **Cost**: $0.44/DPU-hour (~$15/month hourly)
   - **Benefit**: Zero manual metadata management
   - **ROI**: High for large, evolving datasets

## Next Steps

With cataloged data, you're ready for:
- **Lesson 3**: Bronze to Silver transformations
- **Advanced queries**: Join cataloged tables
- **Schema evolution**: Handle changing data structures
- **Multi-engine access**: Query from Athena, EMR, Redshift

---

**Time to Complete**: 15-20 minutes  
**Difficulty**: Intermediate  
**Prerequisites**: Lesson 2 Exercise 2 completed  
**Tools**: Python, boto3 (no Spark required)
