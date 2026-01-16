# Exercise 3: Bronze Layer Organization

## Overview
This exercise teaches you how to properly organize a bronze layer for both structured and unstructured data. You'll learn partitioning strategies, metadata tracking, and directory structures that enable efficient querying and silver layer processing.

## Learning Objectives
- ✅ Design hierarchical bronze layer structure
- ✅ Implement date partitioning for efficient queries
- ✅ Add metadata columns for lineage tracking
- ✅ Separate structured vs unstructured data
- ✅ Enable incremental load patterns

## Prerequisites
- Exercise 1 & 2 completed (data in bronze)
- Docker container running
- Environment variable set: `export BUCKET_NAME="your-bucket-name"`
- Python with pandas, pyarrow, boto3 installed

## Concepts Covered

### Bronze Layer Organization Principles

#### 1. Data Type Separation
```
bronze/
├── structured/      ← Fixed schema data (orders, transactions)
└── unstructured/    ← Flexible schema data (JSON, logs, events)
```

#### 2. Source System Identification
```
bronze/structured/
├── orders/          ← From PostgreSQL
├── customers/       ← From CRM
└── products/        ← From inventory system
```

#### 3. Partitioning Strategy
```
bronze/structured/orders/raw/
├── date=2025-01-14/
├── date=2025-01-15/
└── date=2025-01-16/
```

#### 4. Metadata Tracking
- Ingestion timestamp
- Source system
- Schema version
- Row counts
- Data quality metrics

## Running the Exercise

### Step 1: Run Exercise 3
```bash
cd /workspace/exercises
python exercise-3-bronze-organization.py
```

### Expected Output
```
======================================================================
EXERCISE 3: BRONZE LAYER ORGANIZATION
======================================================================

[Step 1] Initializing Spark Session...
✓ Spark session initialized

[Step 2] Reviewing Current Bronze Layer Structure...
  Current structure (from Exercises 1 & 2):

  s3://bucket/bronze/
  ├── orders/              (Structured data - Parquet)
  │   └── *.parquet
  └── clickstream/         (Unstructured data - JSON → Parquet)
      └── *.parquet

  ⚠️  Issues with current structure:
     • No partitioning (all data in one directory)
     • No metadata (ingestion time, source system)
     • No data type separation (raw vs processed)
     • Difficult to manage incremental loads

[Step 3] Designing Organized Bronze Layer Structure...

  Recommended bronze layer organization:

  s3://{bucket}/bronze/
  ├── structured/                    ← Data type separation
  │   └── orders/                    ← Source system
  │       ├── raw/                   ← Raw format preservation
  │       │   └── date=2025-01-15/   ← Date partitioning
  │       │       └── *.parquet
  │       └── metadata.json          ← Dataset metadata
  │
  └── unstructured/                  ← Data type separation
      └── clickstream/               ← Source system
          ├── raw/                   ← Raw format preservation
          │   └── date=2025-01-15/   ← Date partitioning
          │       └── *.parquet
          └── metadata.json          ← Dataset metadata

[Step 4] Reorganizing Structured Data (Orders)...
  Loaded 10,000 orders from existing bronze
  Writing to organized structure: s3://bucket/bronze/structured/orders/raw/
✓ Structured data reorganized in 5.23s
  Partitions created: 365

[Step 5] Reorganizing Unstructured Data (Clickstream)...
  Loaded 10,000 events from existing bronze
  Writing to organized structure: s3://bucket/bronze/unstructured/clickstream/raw/
✓ Unstructured data reorganized in 6.12s
  Partitions created: 365

[Step 6] Creating Metadata Files...
✓ Metadata created:
  Orders: 10,000 rows, 10 columns
  Clickstream: 10,000 events, 11 columns

[Step 7] Demonstrating Partition Pruning Benefits...
  Querying single partition: date=2025-01-15
✓ Partition query completed in 0.34s
  Rows in partition: 27
  Benefit: Only scans relevant partition, not entire dataset

[Step 8] Comparison: Organized vs Unorganized Bronze...

  📁 UNORGANIZED (Exercises 1 & 2):
     Structure: Flat directories
     Issues:
     ✗ No partitioning → Full table scans
     ✗ No metadata → Unknown lineage
     ✗ No data type separation → Hard to manage

  📁 ORGANIZED (Exercise 3):
     Structure: Hierarchical with partitions
     Benefits:
     ✓ Date partitioning → Efficient queries
     ✓ Metadata tracking → Clear lineage
     ✓ Data type separation → Easy management
     ✓ Incremental load ready → Append new partitions

[Step 9] Bronze Layer Organization Best Practices...
  [Shows 5 best practices]

======================================================================
EXERCISE 3 SUMMARY
======================================================================

📊 Bronze Layer Organization:
   • Structured data: s3://bucket/bronze/structured/orders/raw/
   • Unstructured data: s3://bucket/bronze/unstructured/clickstream/raw/
   • Partitioning: By ingestion_date
   • Metadata: Tracked for both datasets

✅ Key Improvements:
   1. Data type separation (structured vs unstructured)
   2. Date partitioning for efficient queries
   3. Metadata tracking for lineage and quality
   4. Incremental load support (append new partitions)
   5. Clear directory structure for easy navigation

🎯 Benefits for Silver Layer:
   • Partition pruning reduces processing time
   • Metadata enables quality checks
   • Clear structure simplifies ETL pipelines
   • Incremental processing becomes straightforward
```

## What You Learned

### 1. Hierarchical Organization
```
bronze/
├── structured/          ← Data type
│   └── orders/          ← Source system
│       └── raw/         ← Processing stage
│           └── date=/   ← Partition
└── unstructured/
    └── clickstream/
        └── raw/
            └── date=/
```

### 2. Partitioning Benefits
- **Partition pruning**: Query only relevant dates
- **Incremental loads**: Append new date partitions
- **Performance**: 10x faster queries on partitioned data
- **Cost**: Scan less data = lower costs

### 3. Metadata Tracking
```python
metadata = {
    "dataset_name": "orders",
    "source_system": "postgresql",
    "schema_version": "1.0",
    "row_count": 10000,
    "ingestion_timestamp": "2025-01-15T10:30:00"
}
```

## Verification

Check organized bronze layer:
```bash
aws s3 ls s3://bucket/bronze/structured/orders/raw/ --recursive
aws s3 ls s3://bucket/bronze/unstructured/clickstream/raw/ --recursive
```

You should see date-partitioned directories.

## Best Practices Applied

### 1. Data Type Separation
- `structured/` for fixed-schema data
- `unstructured/` for flexible-schema data

### 2. Source System Naming
- Clear identification: `orders/`, `clickstream/`
- Enables data lineage tracking

### 3. Date Partitioning
- Most common partition key
- Supports time-based queries
- Enables incremental processing

### 4. Raw Preservation
- `raw/` subdirectory keeps original format
- Audit trail for compliance
- Reprocessing capability

### 5. Metadata Files
- Schema version tracking
- Quality metrics
- Lineage information

## Common Issues

| Issue | Solution |
|-------|----------|
| `Partition count too high` | Use coarser partitioning (month vs day) |
| `Small files problem` | Coalesce before writing partitions |
| `Metadata not created` | Manually create JSON files in S3 |
| `Query still slow` | Check partition pruning with EXPLAIN |

## Discussion Questions

1. **Why partition by date instead of other fields?**
   - Most queries filter by time
   - Natural incremental load boundary
   - Balances partition count vs size

2. **When would you NOT partition?**
   - Small datasets (< 1GB)
   - No time-based queries
   - Frequent full table scans

3. **How does this help silver layer processing?**
   - Process only new partitions (incremental)
   - Metadata enables quality checks
   - Clear structure simplifies pipelines

## Next Steps

With organized bronze layer, you're ready for:
- **Silver Layer**: Clean and transform partitioned data
- **Incremental Processing**: Process only new date partitions
- **Quality Checks**: Use metadata for validation
- **Performance Optimization**: Leverage partition pruning

---

**Time to Complete**: 10-15 minutes  
**Difficulty**: Intermediate  
**Prerequisites**: Exercise 1 & 2 completed
