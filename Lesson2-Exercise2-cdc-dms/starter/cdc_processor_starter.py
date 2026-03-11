"""
Lesson 2 - Exercise 2: CDC with DMS and Bookmark Management (Starter)

Process DMS full load and CDC files with bookmark tracking.
Uses pandas (no Spark required).
"""

import pandas as pd
import boto3
import json
import os
from datetime import datetime

BUCKET_NAME = os.environ.get('BUCKET_NAME')
s3 = boto3.client('s3', region_name='us-east-1')

def process_full_load(data_dir):
    """Process DMS full load files (LOAD*.parquet)"""
    print("\n[Step 1] Processing Full Load Files...")
    
    # TODO: List all LOAD*.parquet files
    # TODO: Read each file with pandas
    # TODO: Combine into single DataFrame
    # TODO: Return combined data
    
    pass

def read_bookmark(bookmark_path):
    """Read bookmark to track last processed file"""
    print("\n[Step 2] Reading Bookmark...")
    
    # TODO: Read bookmark JSON file
    # TODO: Return last_processed_file and timestamp
    # TODO: Handle case where bookmark doesn't exist (first run)
    
    pass

def list_cdc_files(data_dir, last_processed_file=None):
    """List CDC files after bookmark"""
    print("\n[Step 3] Listing CDC Files...")
    
    # TODO: List all timestamp-based CDC files
    # TODO: Filter to files after last_processed_file
    # TODO: Sort by timestamp
    # TODO: Return list of files to process
    
    pass

def process_cdc_file(filepath):
    """Process single CDC file"""
    # TODO: Read parquet file
    # TODO: Count operations by type (I, U, D)
    # TODO: Return DataFrame and operation counts
    
    pass

def process_cdc_files(cdc_files):
    """Process all CDC files incrementally"""
    print("\n[Step 4] Processing CDC Files...")
    
    all_changes = []
    total_ops = {'I': 0, 'U': 0, 'D': 0}
    
    for filepath in cdc_files:
        # TODO: Process each CDC file
        # TODO: Track operation counts
        # TODO: Accumulate changes
        pass
    
    # TODO: Combine all changes into DataFrame
    # TODO: Return combined data and statistics
    
    pass

def update_bookmark(bookmark_path, last_file, timestamp, record_count):
    """Update bookmark with last processed file"""
    print("\n[Step 5] Updating Bookmark...")
    
    # TODO: Create bookmark dictionary
    # TODO: Write to JSON file
    # TODO: Print confirmation
    
    pass

def analyze_operations(df):
    """Analyze CDC operations"""
    print("\n[Step 6] Analyzing CDC Operations...")
    
    # TODO: Count operations by type
    # TODO: Show examples of each operation type
    # TODO: Print statistics
    
    pass

def demonstrate_incremental_processing():
    """Show how bookmark enables incremental processing"""
    print("\n[Step 7] Demonstrating Incremental Processing...")
    
    # TODO: Simulate second run with new CDC files
    # TODO: Show that only new files are processed
    # TODO: Demonstrate bookmark prevents reprocessing
    
    pass

def print_summary(full_load_count, cdc_count, operations):
    """Print exercise summary"""
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
    print("   1. DMS captures database changes in near real-time")
    print("   2. Bookmarks enable incremental processing")
    print("   3. CDC tracks INSERT, UPDATE, DELETE operations")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 2: CDC WITH DMS")
    print("="*70)
    
    # TODO: Implement exercise steps
    # 1. Process full load files
    # 2. Read bookmark
    # 3. List new CDC files
    # 4. Process CDC files
    # 5. Update bookmark
    # 6. Analyze operations
    # 7. Demonstrate incremental processing
    
    print("\n⚠️  TODO: Implement the exercise steps")
