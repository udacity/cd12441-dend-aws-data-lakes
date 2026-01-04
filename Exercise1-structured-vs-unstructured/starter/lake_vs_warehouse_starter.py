"""
Lesson 1 Starter Code: Data Lake vs Data Warehouse Processing
Student Name: _______________
Date: _______________

Instructions:
1. Complete the TODO sections marked with # TODO: 
2. Run each section and observe the outputs
3. Compare the flexibility differences between approaches
4. Time the operations and fill in the performance comparison
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
# TODO: Load the Parquet file from S3 into a pandas DataFrame
# Hint: Use s3.get_object() and pd.read_parquet()
parquet_obj = # YOUR CODE HERE
structured_df = # YOUR CODE HERE
structured_load_time = time.time() - start_time

print("Structured Data Schema (Warehouse - Schema-on-Write):")
# TODO: Print the column names and data types
print(f"Columns: {# YOUR CODE HERE}")
print(f"Data types:\n{# YOUR CODE HERE}")
print(f"Shape: {# YOUR CODE HERE}")

start_time = time.time()
# TODO: Group by user_id and calculate order_count, total_value, avg_order_value
agg_struct = # YOUR CODE HERE
struct_query_time = time.time() - start_time

struct_count = len(agg_struct)
print(f"Warehouse Results: {struct_count} users processed")
print(f"Load Time: {structured_load_time:.3f}s, Query Time: {struct_query_time:.3f}s\n")

# Task 2: Unstructured Data Processing (Lake Approach)
print("TASK 2: LAKE APPROACH - UNSTRUCTURED DATA")
print("-" * 50)

start_time = time.time()
# TODO: Load JSON lines file from S3 and parse each line
json_obj = # YOUR CODE HERE
raw_logs = # YOUR CODE HERE (Hint: split by '\n' and use json.loads())
unstruct_load_time = time.time() - start_time

print("Raw JSON Schema (Lake - Schema-on-Read):")
# TODO: Examine the structure of the first log entry
print(f"Sample record keys: {# YOUR CODE HERE}")
print(f"Events structure: {# YOUR CODE HERE}")

# Schema-on-read: Flatten nested JSON dynamically
start_time = time.time()
flattened_data = []
# TODO: Loop through raw_logs and flatten the nested events structure
# Extract: user_id, timestamp, each action, page
for log in raw_logs:
    user_id = # YOUR CODE HERE
    timestamp = # YOUR CODE HERE
    events = # YOUR CODE HERE
    
    # TODO: Handle the nested events.actions array
    if isinstance(events, dict) and 'actions' in events:
        for action in # YOUR CODE HERE:
            flattened_data.append({
                # YOUR CODE HERE - create flattened record
            })

flattened_df = pd.DataFrame(flattened_data)
# TODO: Group by user_id and count actions, unique pages
agg_unstruct = # YOUR CODE HERE
unstruct_query_time = time.time() - start_time

unstruct_count = len(agg_unstruct)
print(f"Lake Results: {unstruct_count} users processed")
print(f"Load Time: {unstruct_load_time:.3f}s, Query Time: {unstruct_query_time:.3f}s\n")

# Task 3: Mixed Data Analysis (Lake Advantage)
print("TASK 3: MIXED DATA ANALYSIS - LAKE UNIFIED VIEW")
print("-" * 50)

start_time = time.time()
# TODO: Join the structured and unstructured aggregations
joined_analysis = # YOUR CODE HERE (Hint: use DataFrame.join())
# TODO: Calculate engagement_ratio = action_count / order_count
joined_analysis['engagement_ratio'] = # YOUR CODE HERE
mixed_query_time = time.time() - start_time

mixed_count = len(joined_analysis)
print("Mixed Analysis Results (Top 10 users):")
print(joined_analysis.head(10))
print(f"Mixed Analysis: {mixed_count} users, Query Time: {mixed_query_time:.3f}s\n")

# Task 4: Comparison Summary
print("TASK 4: PERFORMANCE & FLEXIBILITY COMPARISON")
print("-" * 50)

# TODO: Create a comparison DataFrame with the results
comparison_df = pd.DataFrame([
    # YOUR CODE HERE - fill in the comparison data
], columns=["Approach", "Data Type", "Load Time (s)", "Query Time (s)", "Records", "Schema Flexibility"])

print(comparison_df.to_string(index=False))

print("\n=== KEY INSIGHTS ===")
# TODO: Fill in your observations about the differences
print("✓ Lake advantage 1: # YOUR OBSERVATION HERE")
print("✓ Lake advantage 2: # YOUR OBSERVATION HERE") 
print("✓ Warehouse limitation 1: # YOUR OBSERVATION HERE")
print("✓ Warehouse limitation 2: # YOUR OBSERVATION HERE")

# TODO: Calculate and print summary statistics
print(f"\n=== SUMMARY STATISTICS ===")
print(f"Total orders processed: {# YOUR CODE HERE}")
print(f"Total clickstream events: {# YOUR CODE HERE}")
print(f"Users with both orders and clicks: {# YOUR CODE HERE}")
print(f"Average engagement ratio: {# YOUR CODE HERE}")
