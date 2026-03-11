"""
Exercise 2 Starter: Silver Layer Transformation
Student Name: _______________
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, date_trunc
import time
import os

spark = SparkSession.builder \
    .appName("SilverTransformation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== SILVER LAYER: DATA CLEANING AND ENRICHMENT ===\n")

# Load bronze data
bronze_orders = spark.read.parquet("data/orders.parquet")
bronze_clicks = spark.read.json("data/clickstream.json")

start_time = time.time()

# TODO: Clean orders - filter nulls and add date column
silver_orders = bronze_orders.filter(
    # YOUR CODE HERE - filter null order_value and user_id
    # YOUR CODE HERE - filter order_value > 0
).withColumn("order_date", # YOUR CODE HERE - truncate timestamp to day)

# TODO: Flatten clickstream events
silver_events = bronze_clicks.select(
    col("user_id"),
    explode(# YOUR CODE HERE).alias("action"),
    col("events.page").alias("page")
).filter(# YOUR CODE HERE - filter null user_id)

# TODO: Join orders with events
silver = silver_orders.join(
    # YOUR CODE HERE - left outer join on user_id
).dropDuplicates([# YOUR CODE HERE])

process_time = time.time() - start_time

print(f"Silver Orders Count: {silver_orders.count()}")
print(f"Silver Events Count: {silver_events.count()}")
print(f"Silver Joined Count: {silver.count()}")
print(f"Processing Time: {process_time:.3f}s")

# TODO: Show sample data
print("\nSample Silver Data:")
# YOUR CODE HERE

# TODO: Write to output
os.makedirs("output/silver", exist_ok=True)
silver.write.mode(# YOUR CODE HERE).partitionBy(# YOUR CODE HERE).parquet("output/silver/")

spark.stop()
