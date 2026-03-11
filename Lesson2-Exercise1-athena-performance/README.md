# Lesson 2 - Exercise 1: Athena Query Performance Analysis

## Overview
This exercise demonstrates the dramatic performance improvements achieved through proper data organization in S3. You'll query the same dataset using three different organizations from Lesson 1 and measure the performance differences using Amazon Athena.

## Learning Objectives
- ✅ Understand how data organization impacts query performance
- ✅ Measure query execution time and data scanned
- ✅ Compare structured vs unstructured vs partitioned data
- ✅ Learn Athena cost optimization through partition pruning
- ✅ Implement performance benchmarking

## Prerequisites

**Complete Lesson 1 first:**
- Exercise 1: Structured data ingestion
- Exercise 3: Data organization with partitions

**Requirements:**
- AWS account with Athena and S3 access
- Python with pandas, boto3, pyarrow installed
- Environment variable: `export BUCKET_NAME="your-bucket-name"`

## Concepts Covered

### Data Organizations from Lesson 1

#### 1. Structured (Exercise 1)
```
s3://bucket/bronze/orders/
└── orders.parquet  (single file, no partitions)
```
- **Schema**: Fixed, enforced at write
- **Format**: Parquet (columnar)
- **Organization**: Flat, no partitions

#### 2. Unstructured (Exercise 2)
```
s3://bucket/bronze/clickstream/
└── *.parquet  (multiple files, no partitions)
```
- **Schema**: Inferred at read
- **Format**: JSON → Parquet
- **Organization**: Flat, no partitions

#### 3. Organized with Partitions (Exercise 3)
```
s3://bucket/bronze/structured/orders/raw/
└── date=2025-01-15/
    └── *.parquet
```
- **Schema**: Fixed, enforced at write
- **Format**: Parquet (columnar)
- **Organization**: Partitioned by date

### Athena Performance Factors

#### 1. Data Scanned
- **Cost**: $5 per TB scanned
- **Impact**: Partitioning reduces data scanned by 90%+

#### 2. Query Execution Time
- **Factors**: File count, file size, partitioning
- **Optimization**: Fewer, larger files perform better

#### 3. Partition Pruning
- **Definition**: Skip irrelevant partitions based on WHERE clause
- **Benefit**: Scan only necessary data

## Quick Start

### 1. Verify Setup
```bash
export BUCKET_NAME="your-bucket-name"
python setup.py
```

Expected output:
```
[Step 1] Verifying Lesson 1 Data...
  ✓ Lesson 1 - Exercise 1 (Structured)
  ✓ Lesson 1 - Exercise 3 (Organized)

[Step 2] Setting up Athena Output Location...
  ✓ s3://bucket/athena-results/

SETUP COMPLETE
```

### 2. Run Exercise

**Option A: Starter (Learning Mode)**
```bash
cd starter/
python athena_performance_starter.py
```

**Option B: Solution (Reference)**
```bash
cd solution/
python athena_performance_solution.py
```

## Running the Exercise

### Detailed Steps
```bash
cd /Users/emota/Projects/cd12441-dend-aws-data-lakes/Lesson2-Exercise1-athena-performance/starter
python athena_performance_starter.py
```

### Step 2: Expected Output
```
======================================================================
LESSON 2 - EXERCISE 1: ATHENA QUERY PERFORMANCE ANALYSIS
======================================================================

[Step 1] Creating Athena Database...
✓ Database 'lakehouse_lesson2' created

[Step 2] Creating Table for Structured Data (Lesson 1 - Exercise 1)...
  Location: s3://bucket/bronze/orders/
  Organization: Flat, no partitions
✓ Table 'orders_structured' created

[Step 3] Creating Table for Unstructured Data (Lesson 1 - Exercise 2)...
  Location: s3://bucket/bronze/clickstream/
  Organization: Flat, no partitions
✓ Table 'clickstream_unstructured' created

[Step 4] Creating Table for Organized Data (Lesson 1 - Exercise 3)...
  Location: s3://bucket/bronze/structured/orders/raw/
  Organization: Partitioned by date
✓ Table 'orders_organized' created
✓ Partitions loaded (365 partitions)

[Step 5] Benchmark Query 1: Full Table Scan...
  Query: SELECT COUNT(*) FROM orders WHERE order_value > 100

  📊 STRUCTURED (No Partitions):
     Execution Time: 2.34s
     Data Scanned: 1.2 MB
     Rows Returned: 7,543

  📊 ORGANIZED (With Partitions):
     Execution Time: 2.41s
     Data Scanned: 1.2 MB
     Rows Returned: 7,543

  ⚠️  No performance difference - full table scan required

[Step 6] Benchmark Query 2: Date-Filtered Query...
  Query: SELECT * FROM orders WHERE date = '2025-01-15'

  📊 STRUCTURED (No Partitions):
     Execution Time: 2.18s
     Data Scanned: 1.2 MB (entire dataset)
     Rows Returned: 27

  📊 ORGANIZED (With Partitions):
     Execution Time: 0.23s
     Data Scanned: 0.003 MB (single partition)
     Rows Returned: 27

  ✅ PERFORMANCE IMPROVEMENT:
     Speed: 9.5x faster (2.18s → 0.23s)
     Data Scanned: 400x less (1.2 MB → 0.003 MB)
     Cost Savings: 99.75% reduction

[Step 7] Benchmark Query 3: Date Range Query...
  Query: SELECT * FROM orders WHERE date BETWEEN '2025-01-15' AND '2025-01-20'

  📊 STRUCTURED (No Partitions):
     Execution Time: 2.31s
     Data Scanned: 1.2 MB (entire dataset)
     Rows Returned: 162

  📊 ORGANIZED (With Partitions):
     Execution Time: 0.34s
     Data Scanned: 0.018 MB (6 partitions)
     Rows Returned: 162

  ✅ PERFORMANCE IMPROVEMENT:
     Speed: 6.8x faster (2.31s → 0.34s)
     Data Scanned: 67x less (1.2 MB → 0.018 MB)
     Cost Savings: 98.5% reduction

[Step 8] Performance Summary...

  ┌─────────────────────┬──────────────┬──────────────┬──────────────┐
  │ Query Type          │ Structured   │ Organized    │ Improvement  │
  ├─────────────────────┼──────────────┼──────────────┼──────────────┤
  │ Full Table Scan     │ 2.34s        │ 2.41s        │ None         │
  │ Single Date Filter  │ 2.18s        │ 0.23s        │ 9.5x faster  │
  │ Date Range Filter   │ 2.31s        │ 0.34s        │ 6.8x faster  │
  ├─────────────────────┼──────────────┼──────────────┼──────────────┤
  │ Data Scanned (avg)  │ 1.2 MB       │ 0.01 MB      │ 99% less     │
  │ Monthly Cost (1K q) │ $0.006       │ $0.00006     │ 99% savings  │
  └─────────────────────┴──────────────┴──────────────┴──────────────┘

## Expected Results

### Performance Comparison

| Query Type | Structured | Organized | Improvement |
|------------|-----------|-----------|-------------|
| Full Table Scan | 2.3s | 2.4s | None |
| Single Date Filter | 2.2s | 0.2s | **9.5x faster** |
| Date Range Filter | 2.3s | 0.3s | **6.8x faster** |

### Data Scanned

| Query Type | Structured | Organized | Reduction |
|------------|-----------|-----------|-----------|
| Full Table Scan | 1.2 MB | 1.2 MB | 0% |
| Single Date Filter | 1.2 MB | 0.003 MB | **99.75%** |
| Date Range Filter | 1.2 MB | 0.02 MB | **98.3%** |

### Cost Analysis

```
Athena Pricing: $5 per TB scanned

Without Partitions:
  1,000 queries × 1.2 MB = 1.2 GB scanned
  Cost: $0.006

With Partitions:
  1,000 queries × 0.01 MB = 10 MB scanned
  Cost: $0.00006

Savings: 99% ($0.006 → $0.00006)
```

## What You Learned

### 1. Partition Pruning Impact
- **9.5x faster** queries with date filters
- **99% less data scanned** = 99% cost savings
- **No benefit** for full table scans

### 2. When to Partition

| Scenario | Partition? | Reason |
|----------|-----------|--------|
| Time-series data with date filters | ✅ Yes | 90%+ queries filter by date |
| Large datasets (> 1 GB) | ✅ Yes | Significant scan reduction |
| Small datasets (< 100 MB) | ❌ No | Overhead > benefit |
| Mostly full table scans | ❌ No | No pruning benefit |

### 3. Optimal Partition Size
- **Target**: 128 MB - 1 GB per partition
- **Too small**: Metadata overhead
- **Too large**: Less pruning benefit

## Verification

Check Athena query history:
```bash
aws athena list-query-executions --max-results 10
aws athena get-query-execution --query-execution-id <id>
```

## Common Issues

| Issue | Solution |
|-------|----------|
| `Missing Lesson 1 Data` | Complete Lesson 1 Exercise 1 and Exercise 3 first |
| `BUCKET_NAME not set` | Run `export BUCKET_NAME="your-bucket-name"` |
| `Access denied` | Check IAM permissions: S3, Athena, Glue |
| `Table not found` | Re-run `python setup.py` |
| `Query timeout` | Increase Athena timeout or wait for completion |

### Detailed Troubleshooting

**Missing Lesson 1 Data:**
```bash
cd ../Lesson1-Exercise1-structured/
python solution/structured_ingestion_solution.py

cd ../Lesson1-Exercise3-organization/
python solution/bronze_organization_solution.py
```

**Check AWS Credentials:**
```bash
aws sts get-caller-identity
```

**Required IAM Permissions:**
- S3: `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`
- Athena: `athena:StartQueryExecution`, `athena:GetQueryExecution`
- Glue: `glue:CreateDatabase`, `glue:CreateTable`

## Cleanup

Remove all resources:
```bash
# Delete Athena tables
aws athena start-query-execution \
  --query-string "DROP TABLE IF EXISTS lakehouse_lesson2.orders_structured" \
  --result-configuration OutputLocation=s3://${BUCKET_NAME}/athena-results/

aws athena start-query-execution \
  --query-string "DROP TABLE IF EXISTS lakehouse_lesson2.orders_organized" \
  --result-configuration OutputLocation=s3://${BUCKET_NAME}/athena-results/

# Delete database
aws athena start-query-execution \
  --query-string "DROP DATABASE IF EXISTS lakehouse_lesson2 CASCADE" \
  --result-configuration OutputLocation=s3://${BUCKET_NAME}/athena-results/

# Delete Athena results
aws s3 rm s3://${BUCKET_NAME}/athena-results/ --recursive
```

## Discussion Questions

1. **Why doesn't partitioning help full table scans?**
   - Athena must read all partitions anyway
   - Slight overhead from partition metadata
   - Benefit only when pruning partitions

2. **How does this relate to Lesson 1?**
   - Exercise 1: Structured but not partitioned
   - Exercise 3: Organized with partitions ← Best performance

## Next Steps

Proceed to **Lesson 2 - Exercise 2** where you'll:
- Implement CDC (Change Data Capture)
- Use DMS bookmarks for incremental processing
- Build on partitioned data concepts

---

**Time to Complete**: 15-20 minutes  
**Difficulty**: Intermediate  
**Prerequisites**: Lesson 1 Exercise 1 and Exercise 3 completed
