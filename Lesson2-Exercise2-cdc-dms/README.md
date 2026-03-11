# Lesson 2 - Exercise 2: CDC with AWS DMS and Bookmark Management

## Overview
This exercise demonstrates Change Data Capture (CDC) using AWS Database Migration Service (DMS). You'll process both full load and CDC files, implement bookmark management to track processed data, and understand how DMS captures database changes in real-time.

## Learning Objectives
- ✅ Understand CDC (Change Data Capture) concepts
- ✅ Process DMS full load files (LOAD*.parquet)
- ✅ Process DMS CDC files (timestamp-based naming)
- ✅ Implement bookmark management (LSN tracking)
- ✅ Handle INSERT, UPDATE, DELETE operations
- ✅ Build incremental processing pipelines

## Prerequisites
- Lesson 2 Exercise 1 completed (Athena performance)
- Python with pandas, boto3, pyarrow installed
- Environment variable set: `export BUCKET_NAME="your-bucket-name"`
- No Spark required - uses pandas for processing

## Concepts Covered

### Change Data Capture (CDC)
**Definition**: Technique that tracks every INSERT, UPDATE, DELETE in a database and replicates changes to the data lake in near real-time.

**How it works**:
1. **Full Load**: Initial snapshot of all existing data
2. **CDC Mode**: Continuous capture of changes from database logs

### AWS DMS (Database Migration Service)

#### Phase 1: Full Load
```
PostgreSQL RDS → DMS → S3 Bronze Layer
Files: LOAD00000001.parquet, LOAD00000002.parquet
```
- Extracts all existing data
- Hexadecimal file naming
- One-time operation

#### Phase 2: CDC (Continuous)
```
PostgreSQL WAL → DMS → S3 Bronze Layer
Files: 20260103-143000123.parquet, 20260103-143015456.parquet
```
- Reads Write-Ahead Log (WAL)
- Timestamp-based file naming
- Captures INSERT, UPDATE, DELETE
- Runs every 15 minutes (configurable)

### DMS File Structure

#### Full Load Files
```
s3://bucket/dms/swiftshop/orders/
├── LOAD00000001.parquet  ← First batch (500 rows)
├── LOAD00000002.parquet  ← Second batch (500 rows)
├── LOAD00000003.parquet  ← Third batch (500 rows)
└── LOAD00000004.parquet  ← Fourth batch (500 rows)
```

#### CDC Files
```
s3://bucket/dms/swiftshop/orders/
├── 20260103-143000123.parquet  ← 14:30:00 changes
├── 20260103-143015456.parquet  ← 14:30:15 changes
├── 20260103-143030789.parquet  ← 14:30:30 changes
└── 20260103-143045012.parquet  ← 14:30:45 changes
```

### DMS Record Format

Each record includes metadata columns:

```python
{
    'Op': 'I',  # Operation: I=Insert, U=Update, D=Delete
    'order_id': 'order_001',
    'user_id': 'user_123',
    'order_value': 299.99,
    'status': 'pending',
    'order_date': '2026-01-03'
}
```

### Bookmark Management

**Purpose**: Track which files have been processed to enable incremental loads.

**Bookmark Structure**:
```json
{
    "last_processed_file": "20260103-143030789.parquet",
    "last_processed_timestamp": "2026-01-03T14:30:30",
    "records_processed": 15234,
    "last_lsn": "0/1A2B3C4D"
}
```

**LSN (Log Sequence Number)**: PostgreSQL WAL position that DMS tracks to ensure no data loss.

### Database Change Logs by Platform

| Database | Change Log Mechanism |
|----------|---------------------|
| PostgreSQL | Write-Ahead Log (WAL) |
| MySQL/MariaDB | Binary Log (binlog) |
| Oracle | LogMiner + Archive Log |
| SQL Server | Transaction Log + CDC |
| MongoDB | Operations Log (oplog) |

**Key Point**: DMS abstracts these differences - all produce the same Parquet format in S3.

## Running the Exercise

### Step 1: Generate Sample DMS Data (Required)
```bash
cd /Users/emota/Projects/cd12441-dend-aws-data-lakes/Lesson2-Exercise2-cdc-dms/data
python generate_dms_data.py
```

This creates:
- 4 full load files (LOAD*.parquet) - 2000 total records
- 8 CDC files (timestamp-based) - ~55 changes
- Empty bookmark file

### Step 2: Run Exercise
```bash
cd ../starter
export BUCKET_NAME="your-bucket-name"
python cdc_processor_starter.py
```

### Expected Output
```
======================================================================
LESSON 2 - EXERCISE 2: CDC WITH DMS AND BOOKMARK MANAGEMENT
======================================================================

[Step 1] Simulating DMS Full Load...
  DMS connects to PostgreSQL RDS
  Extracts all existing data (2000 orders from last 30 days)
  Writes to S3 as Parquet files

  Full Load Files Created:
  ✓ s3://bucket/dms/swiftshop/orders/LOAD00000001.parquet (500 rows)
  ✓ s3://bucket/dms/swiftshop/orders/LOAD00000002.parquet (500 rows)
  ✓ s3://bucket/dms/swiftshop/orders/LOAD00000003.parquet (500 rows)
  ✓ s3://bucket/dms/swiftshop/orders/LOAD00000004.parquet (500 rows)

  Total: 2000 orders loaded

[Step 2] Processing Full Load Files...
  Reading LOAD00000001.parquet...
  Reading LOAD00000002.parquet...
  Reading LOAD00000003.parquet...
  Reading LOAD00000004.parquet...

  ✓ Full load processed: 2000 records
  ✓ Written to: s3://bucket/bronze/swiftshop/orders/full_load/

[Step 3] Simulating CDC Capture...
  DMS switches to CDC mode
  Reads PostgreSQL Write-Ahead Log (WAL)
  Captures changes every 15 minutes

  CDC Files Created:
  ✓ 20260103-143000123.parquet (15 changes: 10 INSERT, 5 UPDATE)
  ✓ 20260103-143015456.parquet (12 changes: 8 INSERT, 4 UPDATE)
  ✓ 20260103-143030789.parquet (8 changes: 3 INSERT, 3 UPDATE, 2 DELETE)
  ✓ 20260103-143045012.parquet (20 changes: 15 INSERT, 5 UPDATE)

[Step 4] Reading Bookmark...
  Last processed file: None (first run)
  Starting from beginning

[Step 5] Processing CDC Files (Incremental)...
  
  Processing: 20260103-143000123.parquet
    Operations: I=10, U=5, D=0
    Records processed: 15
  
  Processing: 20260103-143015456.parquet
    Operations: I=8, U=4, D=0
    Records processed: 12
  
  Processing: 20260103-143030789.parquet
    Operations: I=3, U=3, D=2
    Records processed: 8
  
  Processing: 20260103-143045012.parquet
    Operations: I=15, U=5, D=0
    Records processed: 20

  ✓ CDC processed: 55 changes
  ✓ Written to: s3://bucket/bronze/swiftshop/orders/cdc/

[Step 6] Updating Bookmark...
  Last processed file: 20260103-143045012.parquet
  Last timestamp: 2026-01-03T14:30:45
  Total records: 55
  Last LSN: 0/1A2B3C4D

  ✓ Bookmark saved to: s3://bucket/bookmarks/swiftshop_orders.json

[Step 7] Demonstrating Incremental Processing...
  
  Run #2: New CDC files arrive
  ✓ 20260103-144000234.parquet (10 changes)
  ✓ 20260103-144015567.parquet (8 changes)

  Reading bookmark...
  Last processed: 20260103-143045012.parquet
  
  Processing only NEW files:
  ✓ 20260103-144000234.parquet (10 changes)
  ✓ 20260103-144015567.parquet (8 changes)

  Skipped 4 already-processed files
  Processed 18 new changes

[Step 8] CDC Operation Breakdown...

  📊 Operation Statistics:
  
  INSERT (I): 46 records (83.6%)
    • New orders created
    • New customers registered
  
  UPDATE (U): 17 records (30.9%)
    • Order status changes (pending → shipped → delivered)
    • Customer address updates
  
  DELETE (D): 2 records (3.6%)
    • Cancelled orders
    • Test data cleanup

[Step 9] Best Practices Demonstrated...

  ✅ 1. Parquet Format
     • 10x faster than JSON for analytics
     • Columnar storage enables efficient queries
     • Built-in compression reduces storage costs

  ✅ 2. Bookmark Management
     • Tracks last processed file
     • Enables incremental processing
     • Prevents duplicate processing
     • Ensures no data loss on restart

  ✅ 3. Partition by Date
     • CDC files naturally partitioned by timestamp
     • Enables efficient time-based queries
     • Supports data retention policies

  ✅ 4. Monitor Replication Lag
     • Track time between database change and S3 arrival
     • Alert if lag exceeds threshold (e.g., 5 minutes)
     • Ensures data lake stays current

======================================================================
EXERCISE 2 SUMMARY
======================================================================

📊 DMS Processing Results:
   • Full Load: 2000 records
   • CDC Changes: 55 records (46 INSERT, 17 UPDATE, 2 DELETE)
   • Total Processed: 2055 records
   • Files Processed: 12 (4 full load + 8 CDC)

🔖 Bookmark Management:
   • Last File: 20260103-143045012.parquet
   • Last Timestamp: 2026-01-03T14:30:45
   • Last LSN: 0/1A2B3C4D
   • Incremental Processing: ✓ Enabled

✅ Key Learnings:
   1. DMS captures database changes in near real-time
   2. Full load provides initial snapshot
   3. CDC tracks INSERT, UPDATE, DELETE operations
   4. Bookmarks enable incremental processing
   5. Same Parquet format regardless of source database

🎯 Real-World Application:
   • SwiftShop: 2000 orders/day, CDC every 15 minutes
   • Replication lag: < 1 minute
   • Data freshness: Near real-time analytics
   • Cost: $0.50/day for DMS replication instance

======================================================================
Next: Lesson 2 - Exercise 3 (Partitioning Optimization)
======================================================================
```

## What You Learned

### 1. DMS Two-Phase Process
- **Full Load**: Initial data extraction (LOAD*.parquet)
- **CDC**: Continuous change capture (timestamp files)

### 2. CDC Operations
```python
'I' = INSERT  # New records
'U' = UPDATE  # Modified records
'D' = DELETE  # Removed records
```

### 3. Bookmark Pattern
```python
bookmark = {
    "last_processed_file": "20260103-143045012.parquet",
    "last_timestamp": "2026-01-03T14:30:45",
    "records_processed": 55
}
```

### 4. Incremental Processing
- Read bookmark
- List S3 files
- Filter to only new files (after bookmark)
- Process new files
- Update bookmark

## Verification

Check processed data:
```bash
aws s3 ls s3://bucket/bronze/swiftshop/orders/full_load/
aws s3 ls s3://bucket/bronze/swiftshop/orders/cdc/
aws s3 cp s3://bucket/bookmarks/swiftshop_orders.json -
```

## Common Issues

| Issue | Solution |
|-------|----------|
| `No CDC files found` | Run generate_dms_data.py first |
| `Bookmark not updating` | Check S3 write permissions |
| `Duplicate processing` | Verify bookmark read logic |
| `Missing operations` | Check 'Op' column in CDC files |

## Discussion Questions

1. **Why use bookmarks instead of reprocessing everything?**
   - Efficiency: Process only new data
   - Cost: Avoid redundant S3 reads
   - Speed: Incremental loads are 100x faster

2. **What happens if bookmark is lost?**
   - Reprocess from beginning (safe but slow)
   - Or use file timestamps to estimate position
   - DMS LSN provides exact restart point

3. **How does this differ from batch ETL?**
   - **Batch**: Daily full extracts (slow, stale data)
   - **CDC**: Continuous incremental updates (fast, fresh data)

## Next Steps

Proceed to **Lesson 2 - Exercise 3** where you'll:
- Optimize partition strategies
- Implement compaction for small files
- Build efficient query patterns

---

**Time to Complete**: 20-25 minutes  
**Difficulty**: Intermediate  
**Prerequisites**: Lesson 2 Exercise 1 completed  
**Tools**: Python, pandas, boto3 (no Spark required)
