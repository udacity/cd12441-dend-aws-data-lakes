"""
Generate sample DMS data for Exercise 2
Creates full load and CDC files simulating AWS DMS output
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os

np.random.seed(42)

def generate_full_load_files(output_dir, num_files=4, rows_per_file=500):
    """Generate DMS full load files (LOAD*.parquet)"""
    print("\n[Generating Full Load Files]")
    
    os.makedirs(output_dir, exist_ok=True)
    
    for i in range(num_files):
        file_num = str(i + 1).zfill(8)
        filename = f"LOAD{file_num}.parquet"
        
        # Generate orders
        orders = []
        for j in range(rows_per_file):
            order_id = f"order_{i * rows_per_file + j:06d}"
            orders.append({
                'order_id': order_id,
                'user_id': f"user_{np.random.randint(1, 1000):05d}",
                'product_id': f"prod_{np.random.randint(1, 100):03d}",
                'order_value': round(np.random.uniform(10, 500), 2),
                'order_date': (datetime.now() - timedelta(days=np.random.randint(0, 30))).date(),
                'status': np.random.choice(['pending', 'shipped', 'delivered'])
            })
        
        df = pd.DataFrame(orders)
        filepath = os.path.join(output_dir, filename)
        df.to_parquet(filepath, index=False)
        print(f"  ✓ {filename} ({len(df)} rows)")
    
    print(f"  Total: {num_files * rows_per_file} orders")

def generate_cdc_files(output_dir, num_files=8):
    """Generate DMS CDC files (timestamp-based naming)"""
    print("\n[Generating CDC Files]")
    
    os.makedirs(output_dir, exist_ok=True)
    
    base_time = datetime(2026, 1, 3, 14, 30, 0)
    
    for i in range(num_files):
        timestamp = base_time + timedelta(seconds=i * 15)
        filename = f"{timestamp.strftime('%Y%m%d-%H%M%S')}{np.random.randint(100, 999)}.parquet"
        
        # Generate CDC records
        num_records = np.random.randint(5, 20)
        records = []
        
        for j in range(num_records):
            op = np.random.choice(['I', 'U', 'D'], p=[0.6, 0.35, 0.05])
            
            record = {
                'Op': op,
                'order_id': f"order_{np.random.randint(0, 2000):06d}",
                'user_id': f"user_{np.random.randint(1, 1000):05d}",
                'product_id': f"prod_{np.random.randint(1, 100):03d}",
                'order_value': round(np.random.uniform(10, 500), 2),
                'order_date': timestamp.date(),
                'status': np.random.choice(['pending', 'shipped', 'delivered', 'cancelled'])
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        filepath = os.path.join(output_dir, filename)
        df.to_parquet(filepath, index=False)
        
        ops_count = df['Op'].value_counts().to_dict()
        print(f"  ✓ {filename} ({len(df)} changes: I={ops_count.get('I', 0)}, U={ops_count.get('U', 0)}, D={ops_count.get('D', 0)})")

def generate_bookmark_file(output_dir):
    """Generate sample bookmark file"""
    print("\n[Generating Bookmark File]")
    
    os.makedirs(output_dir, exist_ok=True)
    
    bookmark = {
        "last_processed_file": None,
        "last_processed_timestamp": None,
        "records_processed": 0,
        "last_lsn": None
    }
    
    filepath = os.path.join(output_dir, "swiftshop_orders.json")
    with open(filepath, 'w') as f:
        json.dump(bookmark, f, indent=2)
    
    print(f"  ✓ swiftshop_orders.json (empty bookmark)")

if __name__ == "__main__":
    print("="*70)
    print("GENERATING DMS SAMPLE DATA")
    print("="*70)
    
    base_dir = os.path.dirname(__file__)
    
    # Generate full load files
    full_load_dir = os.path.join(base_dir, "full_load")
    generate_full_load_files(full_load_dir)
    
    # Generate CDC files
    cdc_dir = os.path.join(base_dir, "cdc")
    generate_cdc_files(cdc_dir)
    
    # Generate bookmark
    bookmark_dir = os.path.join(base_dir, "bookmarks")
    generate_bookmark_file(bookmark_dir)
    
    print("\n" + "="*70)
    print("DATA GENERATION COMPLETE")
    print("="*70)
    print(f"\nFiles created in: {base_dir}")
    print("  • full_load/ - 4 LOAD*.parquet files")
    print("  • cdc/ - 8 timestamp-based CDC files")
    print("  • bookmarks/ - Empty bookmark file")
