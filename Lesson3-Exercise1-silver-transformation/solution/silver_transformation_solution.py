"""
Exercise 1 Solution: Silver Layer Transformation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("SilverTransformation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== SILVER LAYER: DATA CLEANING AND ENRICHMENT ===\n")

bronze_orders = spark.read.parquet("../starter/data/orders.parquet")
bronze_clicks = spark.read.json("../starter/data/clickstream.json")

start_time = time.time()

# Clean orders - rename order_value to revenue, add customer_id
silver_orders = bronze_orders.filter(
    col("order_value").isNotNull() & 
    col("user_id").isNotNull() &
    (col("order_value") > 0)
).withColumn("order_date", col("order_date").cast("date")) \
 .withColumnRenamed("order_value", "revenue") \
 .withColumnRenamed("user_id", "customer_id") \
 .dropDuplicates(["order_id"])

# Clean clickstream
silver_clicks = bronze_clicks.select(
    col("user_id")
).filter(col("user_id").isNotNull()).dropDuplicates()

process_time = time.time() - start_time

print(f"Silver Orders Count: {silver_orders.count()}")
print(f"Silver Clicks Count: {silver_clicks.count()}")
print(f"Processing Time: {process_time:.3f}s")

print("\nSample Silver Orders:")
silver_orders.select("order_id", "customer_id", "product_id", "revenue", "status", "order_date").show(5)

print("\nSample Silver Clicks:")
silver_clicks.show(5)

# Write to output directory
silver_orders.write.mode("overwrite").parquet("output/silver_orders.parquet")
silver_clicks.write.mode("overwrite").parquet("output/silver_clicks.parquet")

print(f"\nSilver data written to output/")

spark.stop()
