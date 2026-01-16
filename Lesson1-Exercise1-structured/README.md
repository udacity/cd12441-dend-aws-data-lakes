# Exercise 1: Bronze Layer - Structured Data Ingestion

## Overview
This exercise demonstrates loading structured data (Parquet format) into the bronze layer of a lakehouse architecture. You'll learn about schema-on-write, raw data preservation, and the characteristics of the bronze layer.

## Learning Objectives
- ✅ Understand schema-on-write with structured data
- ✅ Load Parquet files using PySpark
- ✅ Write data to S3 bronze layer
- ✅ Identify data quality issues in raw data
- ✅ Compare local vs cloud storage performance

## Prerequisites
- Docker container running (`docker exec -it lakehouse-lesson1 bash`)
- AWS credentials configured
- Environment variable set: `export BUCKET_NAME="your-bucket-name"`
- Sample data generated (`orders.parquet` in `/workspace/data/`)
- Python with pandas, pyarrow, boto3 installed

## Concepts Covered

### Schema-on-Write (Structured Data)
- **Definition**: Schema is defined and enforced when data is written
- **Format**: Parquet (columnar, compressed, typed)
- **Benefits**: Fast queries, type safety, efficient storage
- **Trade-off**: Less flexible, requires upfront schema design

### Bronze Layer
- **Purpose**: Raw data landing zone
- **Strategy**: Store data "as-is" without transformation
- **Quality**: Contains all original issues (nulls, duplicates, errors)
- **Pattern**: Append-only for audit trail

## Running the Exercise

### Step 1: Enter Container
```bash
cd /Users/emota/Projects/udacity-data-course/lesson-1/infrastructure
docker-compose up -d
docker exec -it lakehouse-lesson1 bash
```

### Step 2: Set Environment
```bash
export BUCKET_NAME="lakehouse-lesson1-yourname-123456789"
cd /workspace/exercises
```

### Step 3: Run Exercise
```bash
python exercise-1-bronze.py
```

### Expected Output
```
======================================================================
EXERCISE 1: BRONZE LAYER - STRUCTURED DATA INGESTION
======================================================================

[Step 1] Initializing AWS S3 Connection...
✓ S3 client initialized

[Step 2] Loading structured data (orders.parquet)...
  Source: /workspace/data/orders.parquet
  Format: Parquet (columnar, schema-on-write)
✓ Data loaded in 0.12 seconds
  Rows: 10,000
  Columns: 6

[Step 3] Examining Schema (Schema-on-Write)...
  Parquet enforces schema at write time - explicit data types:

order_id               object
user_id                object
product_id             object
order_value           float64
order_date     datetime64[ns]
status                 object

[Step 4] Preview Sample Data...
       order_id     user_id product_id  order_value order_date     status
0  order_000000  user_00024   prod_088       259.67 2025-02-28  completed
1  order_000001  user_00697   prod_015       328.44 2025-05-31  completed
...

[Step 5] Data Quality Assessment (Raw Bronze Layer)...
  Bronze layer contains data AS-IS with quality issues:

  Total rows: 10,000
  Null order_value: 522 (5.2%)
  Duplicate order_ids: 210 (2.1%)

  ⚠️  These issues will be cleaned in later exercises (Silver layer)

[Step 6] Writing to S3 Bronze Layer...
  Destination: s3://lakehouse-lesson1-yourname-123456789/bronze/orders/orders.parquet
  Strategy: Append-only (raw data preservation)
✓ Data written to S3 in 0.87 seconds

[Step 7] Verifying S3 Bronze Layer...
✓ Verification successful
  Rows in S3: 10,000
  Match: ✓

======================================================================
EXERCISE 1 SUMMARY
======================================================================

📊 Structured Data (Parquet):
   • Schema: Explicit, enforced at write time
   • Load time: 0.12s
   • Write time: 0.87s
   • Total rows: 10,000

🗂️  Bronze Layer Characteristics:
   • Raw data preserved as-is
   • Contains quality issues (nulls, duplicates)
   • Append-only strategy
   • Location: s3://lakehouse-lesson1-yourname-123456789/bronze/orders/orders.parquet

✅ Key Takeaway:
   Structured data (Parquet) has explicit schema enforced at write time,
   enabling fast, type-safe queries. Bronze layer stores raw data with
   all quality issues intact for downstream cleaning.

======================================================================
Next: Exercise 2 - Unstructured Data (JSON)
======================================================================
```

## What You Learned

### 1. Schema-on-Write Benefits
- **Type Safety**: Parquet enforces data types (string, double, timestamp)
- **Fast Reads**: Columnar format enables efficient queries
- **Compression**: Smaller file size vs CSV/JSON

### 2. Bronze Layer Pattern
- **Raw Preservation**: Data stored exactly as received
- **Quality Issues**: Nulls (5.2%) and duplicates (2.1%) preserved
- **Audit Trail**: Original data always available

### 3. Performance Observations
- **Local Read**: ~0.5 seconds for 10K rows
- **S3 Write**: ~3 seconds (network transfer)
- **Verification**: Instant read from S3

## Verification

Check your S3 bucket to see the bronze layer:

```bash
aws s3 ls s3://lakehouse-lesson1-yourname-123456789/bronze/orders/
```

You should see Parquet files created by Spark.

## Common Issues

| Issue | Solution |
|-------|----------|
| `BUCKET_NAME not set` | Run `export BUCKET_NAME="your-bucket-name"` |
| `Access Denied (S3)` | Check AWS credentials: `aws sts get-caller-identity` |
| `File not found` | Verify data generated: `ls /workspace/data/orders.parquet` |
| `Pandas error` | Restart container: `docker-compose restart` |
| `Module not found` | Rebuild: `docker-compose build --no-cache` |

## Discussion Questions

1. **Why preserve data quality issues in bronze?**
   - Maintains audit trail of original data
   - Allows reprocessing with different cleaning rules
   - Enables debugging of upstream data sources

2. **Why use Parquet instead of CSV?**
   - Columnar storage = faster queries
   - Built-in compression = smaller files
   - Schema enforcement = type safety

3. **What's the trade-off of schema-on-write?**
   - **Pro**: Fast queries, type safety
   - **Con**: Less flexible, requires upfront schema design

## Next Steps

Proceed to **Exercise 2: Silver Layer** where you'll:
- Clean nulls and duplicates
- Standardize data formats
- Enrich with business logic
- Create production-ready datasets

---

**Time to Complete**: 5-10 minutes  
**Difficulty**: Beginner  
**Prerequisites**: Basic Python, SQL concepts
