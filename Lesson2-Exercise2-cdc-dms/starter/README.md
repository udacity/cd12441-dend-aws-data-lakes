# Lesson 2 - Exercise 2: CDC with DMS (Starter Guide)

## Objective
Understand AWS DMS Change Data Capture by processing full load and CDC files with bookmark management using pandas.

## Instructions

### Step 1: Generate Sample Data
```bash
cd data/
python generate_dms_data.py
```

This creates:
- `full_load/LOAD*.parquet` - 4 files, 2000 total records
- `cdc/*.parquet` - 8 timestamp-based CDC files
- `bookmarks/swiftshop_orders.json` - Empty bookmark

### Step 2: Implement Starter Code

Open `cdc_processor_starter.py` and complete the TODOs:

#### TODO 1: Process Full Load Files
```python
def process_full_load(data_dir):
    load_files = sorted(glob(os.path.join(data_dir, "full_load", "LOAD*.parquet")))
    
    dfs = []
    for filepath in load_files:
        df = pd.read_parquet(filepath)
        print(f"  ✓ {os.path.basename(filepath)} ({len(df)} rows)")
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    return combined
```

#### TODO 2: Read Bookmark
```python
def read_bookmark(bookmark_path):
    if not os.path.exists(bookmark_path):
        return None, None
    
    with open(bookmark_path, 'r') as f:
        bookmark = json.load(f)
    
    return bookmark.get('last_processed_file'), bookmark.get('last_processed_timestamp')
```

#### TODO 3: List CDC Files After Bookmark
```python
def list_cdc_files(data_dir, last_processed_file=None):
    cdc_files = sorted(glob(os.path.join(data_dir, "cdc", "*.parquet")))
    
    if last_processed_file:
        # Filter to files after bookmark
        cdc_files = [f for f in cdc_files if os.path.basename(f) > last_processed_file]
    
    return cdc_files
```

#### TODO 4: Process CDC Files
```python
def process_cdc_files(cdc_files):
    all_changes = []
    total_ops = {'I': 0, 'U': 0, 'D': 0}
    
    for filepath in cdc_files:
        df = pd.read_parquet(filepath)
        
        # Count operations
        ops = df['Op'].value_counts().to_dict()
        for op, count in ops.items():
            total_ops[op] = total_ops.get(op, 0) + count
        
        all_changes.append(df)
    
    combined = pd.concat(all_changes, ignore_index=True) if all_changes else pd.DataFrame()
    return combined, total_ops
```

#### TODO 5: Update Bookmark
```python
def update_bookmark(bookmark_path, last_file, record_count):
    # Extract timestamp from filename
    timestamp_str = last_file.split('.')[0][:15]
    timestamp = datetime.strptime(timestamp_str, '%Y%m%d-%H%M%S').isoformat()
    
    bookmark = {
        "last_processed_file": last_file,
        "last_processed_timestamp": timestamp,
        "records_processed": record_count,
        "last_lsn": "0/1A2B3C4D"
    }
    
    with open(bookmark_path, 'w') as f:
        json.dump(bookmark, f, indent=2)
```

### Step 3: Run the Exercise
```bash
cd starter/
python cdc_processor_starter.py
```

### Step 4: Verify Results

Check bookmark was updated:
```bash
cat ../data/bookmarks/swiftshop_orders.json
```

Expected output:
```json
{
  "last_processed_file": "20260103-143045012.parquet",
  "last_processed_timestamp": "2026-01-03T14:30:45",
  "records_processed": 55,
  "last_lsn": "0/1A2B3C4D"
}
```

### Step 5: Test Incremental Processing

Run again to verify bookmark prevents reprocessing:
```bash
python cdc_processor_starter.py
```

Should show: "Found 0 new files after 20260103-143045012.parquet"

## Expected Results

### Full Load Processing
```
[Step 1] Processing Full Load Files...
  ✓ LOAD00000001.parquet (500 rows)
  ✓ LOAD00000002.parquet (500 rows)
  ✓ LOAD00000003.parquet (500 rows)
  ✓ LOAD00000004.parquet (500 rows)

  Total: 2000 records
```

### CDC Processing
```
[Step 4] Processing CDC Files...
  ✓ 20260103-143000123.parquet (15 changes: I=10, U=5, D=0)
  ✓ 20260103-143015456.parquet (12 changes: I=8, U=4, D=0)
  ...

  Total: 55 changes
  Operations: INSERT=46, UPDATE=17, DELETE=2
```

## Key Concepts

### DMS File Naming

**Full Load**: `LOAD{hex_counter}.parquet`
- LOAD00000001.parquet
- LOAD00000002.parquet

**CDC**: `{YYYYMMDD-HHMMSS}{random}.parquet`
- 20260103-143000123.parquet
- 20260103-143015456.parquet

### CDC Operations

| Op | Meaning | Example |
|----|---------|---------|
| I | INSERT | New order created |
| U | UPDATE | Order status changed |
| D | DELETE | Order cancelled |

### Bookmark Structure

```json
{
  "last_processed_file": "20260103-143045012.parquet",
  "last_processed_timestamp": "2026-01-03T14:30:45",
  "records_processed": 55,
  "last_lsn": "0/1A2B3C4D"
}
```

**LSN (Log Sequence Number)**: PostgreSQL WAL position that DMS tracks.

## Common Issues

| Issue | Solution |
|-------|----------|
| `No such file or directory` | Run generate_dms_data.py first |
| `Empty DataFrame` | Check CDC files exist in data/cdc/ |
| `Bookmark not updating` | Verify write permissions |
| `All files reprocessed` | Check bookmark read logic |

## Discussion Questions

1. **Why separate full load and CDC?**
   - Full load: Initial snapshot (one-time)
   - CDC: Continuous updates (ongoing)
   - Different file naming conventions

2. **What happens if DMS restarts?**
   - Reads LSN from bookmark
   - Resumes from exact WAL position
   - No data loss or duplication

3. **How often should CDC run?**
   - Trade-off: Freshness vs cost
   - SwiftShop: Every 15 minutes
   - High-frequency: Every 1-5 minutes
   - Batch: Hourly or daily

## Next Steps

After completing this exercise:
- Understand DMS two-phase process
- Implement bookmark-based incremental processing
- Handle INSERT, UPDATE, DELETE operations
- Ready for Lesson 2 Exercise 3 (Partitioning Optimization)

---

**Time to Complete**: 20-25 minutes  
**Difficulty**: Intermediate  
**Tools**: Python, pandas (no Spark)
