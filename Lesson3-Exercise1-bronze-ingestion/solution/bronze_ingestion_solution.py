"""
Exercise 1 Solution: Bronze Layer Ingestion with PySpark
"""

from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== BRONZE LAYER: RAW DATA INGESTION ===\n")

start_time = time.time()

# Load structured orders
bronze_orders = spark.read.parquet("data/orders.parquet")

# Load unstructured clickstream
bronze_clicks = spark.read.json("data/clickstream.json")

load_time = time.time() - start_time

print("Bronze Orders Schema:")
bronze_orders.printSchema()
print(f"Bronze Orders Count: {bronze_orders.count()}")

print("\nBronze Clickstream Schema:")
bronze_clicks.printSchema()
print(f"Bronze Clickstream Count: {bronze_clicks.count()}")

print(f"\nBronze Load Time: {load_time:.3f}s")

print("\nSample Orders:")
bronze_orders.show(5)

print("\nSample Clickstream:")
bronze_clicks.show(5, truncate=False)

spark.stop()
