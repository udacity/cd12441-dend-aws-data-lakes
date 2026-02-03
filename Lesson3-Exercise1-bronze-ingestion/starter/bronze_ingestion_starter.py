"""
Exercise 1 Starter: Bronze Layer Ingestion with PySpark
Student Name: _______________
"""

from pyspark.sql import SparkSession
import time

# TODO: Initialize Spark session
spark = SparkSession.builder \
    .appName(# YOUR CODE HERE) \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== BRONZE LAYER: RAW DATA INGESTION ===\n")

start_time = time.time()

# TODO: Load structured orders from Parquet
bronze_orders = # YOUR CODE HERE

# TODO: Load unstructured clickstream from JSON
bronze_clicks = # YOUR CODE HERE

load_time = time.time() - start_time

# TODO: Display schemas
print("Bronze Orders Schema:")
# YOUR CODE HERE

print(f"Bronze Orders Count: {# YOUR CODE HERE}")

print("\nBronze Clickstream Schema:")
# YOUR CODE HERE

print(f"Bronze Clickstream Count: {# YOUR CODE HERE}")

print(f"\nBronze Load Time: {load_time:.3f}s")

# TODO: Show sample data
print("\nSample Orders:")
# YOUR CODE HERE

print("\nSample Clickstream:")
# YOUR CODE HERE

# TODO: Stop Spark session
# YOUR CODE HERE
