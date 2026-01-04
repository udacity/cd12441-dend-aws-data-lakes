"""
Lesson 1 Solution: Data Lake vs Data Warehouse Processing
Complete implementation demonstrating schema-on-read vs schema-on-write
"""

import pandas as pd
import json
import boto3
import time
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')
bucket = 'cloudmart'

print("=== LESSON 1: DATA LAKE vs DATA WAREHOUSE PROCESSING ===\n")

# Task 1: Structured Data Processing (Warehouse Approach)
print("TASK 1: WAREHOUSE APPROACH - STRUCTURED DATA")
print("-" * 50)

start_time = time.time()
# Load structured Parquet data
parquet_obj = s3.get_object(Bucket=bucket, Key='demo/orders.parquet')
structured_df = pd.read_parquet(BytesIO(parquet_obj['Body'].read()))
structured_load_time = time.time() - start_time

print("Structured Data Schema (Warehouse - Schema-on-Write):")
print(f"Columns: {list(structured_df.columns)}")
print(f"Data types:\n{structured_df.dtypes}")
print(f"Shape: {structured_df.shape}")

start_time = time.time()
agg_struct = structured_df.groupby('user_id').agg({
    'order_id': 'count',
    'order_value': ['sum', 'mean']
}).round(2)
agg_struct.columns = ['order_count', 'total_value', 'avg_order_value']
struct_query_time = time.time() - start_time

struct_count = len(agg_struct)
print(f"Warehouse Results: {struct_count} users processed")
print(f"Load Time: {structured_load_time:.3f}s, Query Time: {struct_query_time:.3f}s\n")

# Task 2: Unstructured Data Processing (Lake Approach)
print("TASK 2: LAKE APPROACH - UNSTRUCTURED DATA")
print("-" * 50)

start_time = time.time()
# Load unstructured JSON data
json_obj = s3.get_object(Bucket=bucket, Key='demo/clickstream.json')
raw_logs = [json.loads(line) for line in json_obj['Body'].read().decode().strip().split('\n')]
unstruct_load_time = time.time() - start_time

print("Raw JSON Schema (Lake - Schema-on-Read):")
print(f"Sample record keys: {list(raw_logs[0].keys())}")
print(f"Events structure: {type(raw_logs[0].get('events', {}))}")

# Schema-on-read: Flatten nested JSON dynamically
start_time = time.time()
flattened_data = []
for log in raw_logs:
    user_id = log.get('user_id')
    timestamp = log.get('timestamp')
    events = log.get('events', {})
    
    if isinstance(events, dict) and 'actions' in events:
        for action in events.get('actions', []):
            flattened_data.append({
                'user_id': user_id,
                'timestamp': timestamp,
                'action': action,
                'page': events.get('page')
            })

flattened_df = pd.DataFrame(flattened_data)
agg_unstruct = flattened_df.groupby('user_id').agg({
    'action': 'count',
    'page': 'nunique'
})
agg_unstruct.columns = ['action_count', 'unique_pages']
unstruct_query_time = time.time() - start_time

unstruct_count = len(agg_unstruct)
print(f"Lake Results: {unstruct_count} users processed")
print(f"Load Time: {unstruct_load_time:.3f}s, Query Time: {unstruct_query_time:.3f}s\n")

# Task 3: Mixed Data Analysis (Lake Advantage)
print("TASK 3: MIXED DATA ANALYSIS - LAKE UNIFIED VIEW")
print("-" * 50)

start_time = time.time()
# Join structured and unstructured data
joined_analysis = agg_struct.join(agg_unstruct, how='inner')
joined_analysis['engagement_ratio'] = (joined_analysis['action_count'] / 
                                     joined_analysis['order_count']).round(2)
mixed_query_time = time.time() - start_time

mixed_count = len(joined_analysis)
print("Mixed Analysis Results (Top 10 users):")
print(joined_analysis.head(10))
print(f"Mixed Analysis: {mixed_count} users, Query Time: {mixed_query_time:.3f}s\n")

# Task 4: Comparison Summary
print("TASK 4: PERFORMANCE & FLEXIBILITY COMPARISON")
print("-" * 50)

comparison_df = pd.DataFrame([
    ["Warehouse", "Structured", f"{structured_load_time:.3f}", f"{struct_query_time:.3f}", struct_count, "Rigid"],
    ["Lake", "Unstructured", f"{unstruct_load_time:.3f}", f"{unstruct_query_time:.3f}", unstruct_count, "Dynamic"],
    ["Lake", "Mixed", "N/A", f"{mixed_query_time:.3f}", mixed_count, "Unified"]
], columns=["Approach", "Data Type", "Load Time (s)", "Query Time (s)", "Records", "Schema Flexibility"])

print(comparison_df.to_string(index=False))

print("\n=== KEY INSIGHTS ===")
print("✓ Lake handles variable JSON schemas without predefined structure")
print("✓ Lake enables unified analysis across structured/unstructured data")
print("✓ Warehouse requires fixed schema, fails on schema evolution")
print("✓ Warehouse cannot process raw unstructured data without ETL")

print(f"\n=== SUMMARY STATISTICS ===")
print(f"Total orders processed: {structured_df['order_id'].count()}")
print(f"Total clickstream events: {len(flattened_data)}")
print(f"Users with both orders and clicks: {mixed_count}")
print(f"Average engagement ratio: {joined_analysis['engagement_ratio'].mean():.2f}")
