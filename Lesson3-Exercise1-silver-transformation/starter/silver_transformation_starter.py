"""
Exercise 1 Starter: Silver Layer Transformation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, date_trunc, from_unixtime
import time
import os
import tempfile

spark = SparkSession.builder \
    .appName("SilverTransformation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

print("=== SILVER LAYER: DATA CLEANING AND ENRICHMENT ===\n")

bronze_orders = spark.read.parquet("data/orders.parquet")
bronze_clicks = spark.read.json("data/clickstream.json")

start_time = time.time()

# TODO: Clean orders - filter nulls, rename order_value to revenue, rename user_id to customer_id
# Hint: filter order_value isNotNull, user_id isNotNull, order_value > 0
# Hint: cast order_date to date type
# Hint: use withColumnRenamed for renaming
# Hint: use dropDuplicates on order_id
silver_orders = bronze_orders  # YOUR CODE HERE

# TODO: Clean clickstream - select user_id, filter nulls, deduplicate
silver_clicks = bronze_clicks  # YOUR CODE HERE

process_time = time.time() - start_time

print(f"Silver Orders Count: {silver_orders.count()}")
print(f"Silver Clicks Count: {silver_clicks.count()}")
print(f"Processing Time: {process_time:.3f}s")

print("\nSample Silver Orders:")
# TODO: Show first 5 rows with selected columns
# YOUR CODE HERE

print("\nSample Silver Clicks:")
# YOUR CODE HERE

# TODO: Write to output directory
# YOUR CODE HERE

print(f"\nSilver data written to output/")

spark.stop()
