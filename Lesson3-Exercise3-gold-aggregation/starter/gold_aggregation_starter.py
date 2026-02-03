"""
Exercise 3 Starter: Gold Layer Aggregation
Student Name: _______________
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count, desc
import pandas as pd
import time
import os

spark = SparkSession.builder \
    .appName("GoldAggregation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== GOLD LAYER: BUSINESS KPIs ===\n")

# TODO: Load silver data
silver = # YOUR CODE HERE

start_time = time.time()

# TODO: Create gold KPIs - aggregate by product_id and order_date
gold_kpis = silver.groupBy(# YOUR CODE HERE) \
                  .agg(
                      # YOUR CODE HERE - sum order_value as total_revenue
                      # YOUR CODE HERE - count actions as total_events
                      # YOUR CODE HERE - count orders as total_orders
                  ) \
                  .orderBy(# YOUR CODE HERE - descending by revenue)

process_time = time.time() - start_time

print(f"Gold KPIs Count: {gold_kpis.count()}")
print(f"Processing Time: {process_time:.3f}s")

# TODO: Write gold layer as CSV
os.makedirs("output/gold", exist_ok=True)
gold_kpis.coalesce(# YOUR CODE HERE).write.mode(# YOUR CODE HERE).option("header", "true").csv("output/gold/")

# TODO: Generate top 10 report
gold_pd = gold_kpis.limit(# YOUR CODE HERE).toPandas()
print("\nTop 10 Product KPIs:")
print(# YOUR CODE HERE - convert to markdown)

# TODO: Business insights
print(f"\n=== BUSINESS INSIGHTS ===")
print(f"Average product revenue: ${# YOUR CODE HERE}")
print(f"Top revenue product: ${# YOUR CODE HERE}")
print(f"Revenue range: ${# YOUR CODE HERE} - ${# YOUR CODE HERE}")

spark.stop()
