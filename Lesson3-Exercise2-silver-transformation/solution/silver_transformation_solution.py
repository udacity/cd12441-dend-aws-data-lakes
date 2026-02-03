"""
Exercise 2 Solution: Silver Layer Transformation
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

bronze_orders = spark.read.parquet("data/orders.parquet")
bronze_clicks = spark.read.json("data/clickstream.json")

start_time = time.time()

# Clean orders
silver_orders = bronze_orders.filter(
    col("order_value").isNotNull() & 
    col("user_id").isNotNull() &
    (col("order_value") > 0)
).withColumn("order_date", date_trunc("day", col("order_timestamp")))

# Flatten clickstream
silver_events = bronze_clicks.select(
    col("user_id"),
    explode(col("events.actions")).alias("action"),
    col("events.page").alias("page")
).filter(col("user_id").isNotNull())

# Join
silver = silver_orders.join(silver_events, "user_id", "left_outer") \
                     .dropDuplicates(["order_id"])

process_time = time.time() - start_time

print(f"Silver Orders Count: {silver_orders.count()}")
print(f"Silver Events Count: {silver_events.count()}")
print(f"Silver Joined Count: {silver.count()}")
print(f"Processing Time: {process_time:.3f}s")

print("\nSample Silver Data:")
silver.select("order_id", "user_id", "product_id", "order_value", "action", "order_date").show(5)

os.makedirs("output/silver", exist_ok=True)
silver.write.mode("overwrite").partitionBy("order_date").parquet("output/silver/")

print("\nSilver data written to output/silver/")

spark.stop()
