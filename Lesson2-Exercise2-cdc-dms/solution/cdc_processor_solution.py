"""
Lesson 2 - Exercise 2: CDC with DMS and Bookmark Management (Solution)

Complete implementation using pandas (no Spark).
"""

import pandas as pd
import json
import os
from glob import glob
from datetime import datetime

def process_full_load(data_dir):
    """Process DMS full load files"""
    print("\n[Step 1] Processing Full Load Files...")
    
    load_files = sorted(glob(os.path.join(data_dir, "full_load", "LOAD*.parquet")))
    
    if not load_files:
        print("  ⚠️  No full load files found")
        print(f"  Run: cd {data_dir} && python generate_dms_data.py")
        return pd.DataFrame()
    
    dfs = []
    for filepath in load_files:
        df = pd.read_parquet(filepath)
        print(f"  ✓ {os.path.basename(filepath)} ({len(df)} rows)")
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    print(f"\n  Total: {len(combined)} records")
    return combined

def read_bookmark(bookmark_path):
    """Read bookmark file"""
    print("\n[Step 2] Reading Bookmark...")
    
    if not os.path.exists(bookmark_path):
        print("  ⚠️  No bookmark found (first run)")
        return None, None
    
    with open(bookmark_path, 'r') as f:
        bookmark = json.load(f)
    
    last_file = bookmark.get('last_processed_file')
    last_ts = bookmark.get('last_processed_timestamp')
    
    if last_file:
        print(f"  Last processed: {last_file}")
        print(f"  Timestamp: {last_ts}")
    else:
        print("  Empty bookmark (first run)")
    
    return last_file, last_ts

def list_cdc_files(data_dir, last_processed_file=None):
    """List CDC files after bookmark"""
    print("\n[Step 3] Listing CDC Files...")
    
    cdc_files = sorted(glob(os.path.join(data_dir, "cdc", "*.parquet")))
    
    if last_processed_file:
        # Filter to files after bookmark
        cdc_files = [f for f in cdc_files if os.path.basename(f) > last_processed_file]
        print(f"  Found {len(cdc_files)} new files after {last_processed_file}")
    else:
        print(f"  Found {len(cdc_files)} CDC files (processing all)")
    
    return cdc_files

def process_cdc_files(cdc_files):
    """Process CDC files"""
    print("\n[Step 4] Processing CDC Files...")
    
    if not cdc_files:
        print("  ⚠️  No CDC files to process")
        return pd.DataFrame(), {'I': 0, 'U': 0, 'D': 0}
    
    all_changes = []
    total_ops = {'I': 0, 'U': 0, 'D': 0}
    
    for filepath in cdc_files:
        df = pd.read_parquet(filepath)
        filename = os.path.basename(filepath)
        
        ops = df['Op'].value_counts().to_dict()
        for op, count in ops.items():
            total_ops[op] = total_ops.get(op, 0) + count
        
        print(f"  ✓ {filename} ({len(df)} changes: I={ops.get('I', 0)}, U={ops.get('U', 0)}, D={ops.get('D', 0)})")
        all_changes.append(df)
    
    combined = pd.concat(all_changes, ignore_index=True) if all_changes else pd.DataFrame()
    
    print(f"\n  Total: {len(combined)} changes")
    print(f"  Operations: INSERT={total_ops.get('I', 0)}, UPDATE={total_ops.get('U', 0)}, DELETE={total_ops.get('D', 0)}")
    
    return combined, total_ops

def update_bookmark(bookmark_path, last_file, record_count):
    """Update bookmark"""
    print("\n[Step 5] Updating Bookmark...")
    
    # Extract timestamp from filename (YYYYMMDD-HHMMSS)
    timestamp_str = last_file.split('.')[0][:15]
    timestamp = datetime.strptime(timestamp_str, '%Y%m%d-%H%M%S').isoformat()
    
    bookmark = {
        "last_processed_file": last_file,
        "last_processed_timestamp": timestamp,
        "records_processed": record_count,
        "last_lsn": "0/1A2B3C4D"  # Simulated LSN
    }
    
    os.makedirs(os.path.dirname(bookmark_path), exist_ok=True)
    with open(bookmark_path, 'w') as f:
        json.dump(bookmark, f, indent=2)
    
    print(f"  ✓ Last file: {last_file}")
    print(f"  ✓ Timestamp: {timestamp}")
    print(f"  ✓ Records: {record_count}")

def analyze_operations(df):
    """Analyze CDC operations"""
    print("\n[Step 6] Analyzing CDC Operations...")
    
    if df.empty:
        print("  No CDC data to analyze")
        return
    
    ops = df['Op'].value_counts()
    total = len(df)
    
    print(f"\n  📊 Operation Breakdown:")
    for op, count in ops.items():
        pct = (count / total) * 100
        op_name = {'I': 'INSERT', 'U': 'UPDATE', 'D': 'DELETE'}.get(op, op)
        print(f"     {op_name}: {count} ({pct:.1f}%)")
    
    # Show examples
    print(f"\n  Example INSERT:")
    insert_example = df[df['Op'] == 'I'].head(1)
    if not insert_example.empty:
        print(f"     {insert_example.to_dict('records')[0]}")
    
    print(f"\n  Example UPDATE:")
    update_example = df[df['Op'] == 'U'].head(1)
    if not update_example.empty:
        print(f"     {update_example.to_dict('records')[0]}")

def print_summary(full_load_count, cdc_count, operations):
    """Print summary"""
    print("\n" + "="*70)
    print("EXERCISE 2 SUMMARY")
    print("="*70)
    print(f"\n📊 DMS Processing Results:")
    print(f"   • Full Load: {full_load_count} records")
    print(f"   • CDC Changes: {cdc_count} records")
    print(f"   • INSERT: {operations.get('I', 0)}")
    print(f"   • UPDATE: {operations.get('U', 0)}")
    print(f"   • DELETE: {operations.get('D', 0)}")
    print("\n✅ Key Learnings:")
    print("   1. DMS two-phase process: Full Load + CDC")
    print("   2. Bookmarks enable incremental processing")
    print("   3. CDC captures INSERT, UPDATE, DELETE operations")
    print("   4. Parquet format works across all database platforms")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 2: CDC WITH DMS")
    print("="*70)
    
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    bookmark_path = os.path.join(data_dir, "bookmarks", "swiftshop_orders.json")
    
    # Check if data exists
    if not os.path.exists(os.path.join(data_dir, "full_load")):
        print("\n❌ Error: Sample data not found")
        print(f"\n   Generate data first:")
        print(f"   cd {data_dir}")
        print(f"   python generate_dms_data.py")
        exit(1)
    
    # Step 1: Process full load
    full_load_df = process_full_load(data_dir)
    
    # Step 2: Read bookmark
    last_file, last_ts = read_bookmark(bookmark_path)
    
    # Step 3: List new CDC files
    cdc_files = list_cdc_files(data_dir, last_file)
    
    # Step 4: Process CDC files
    cdc_df, operations = process_cdc_files(cdc_files)
    
    # Step 5: Update bookmark
    if cdc_files:
        last_processed = os.path.basename(cdc_files[-1])
        update_bookmark(bookmark_path, last_processed, len(cdc_df))
    
    # Step 6: Analyze operations
    analyze_operations(cdc_df)
    
    # Step 7: Print summary
    print_summary(len(full_load_df), len(cdc_df), operations)
