"""
Exercise 3 Solution: Gold Layer Aggregation
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

silver = spark.read.parquet("output/silver/")

start_time = time.time()

gold_kpis = silver.groupBy("product_id", "order_date") \
                  .agg(
                      spark_sum("order_value").alias("total_revenue"),
                      count("action").alias("total_events"),
                      count("order_id").alias("total_orders")
                  ) \
                  .orderBy(desc("total_revenue"))

process_time = time.time() - start_time

print(f"Gold KPIs Count: {gold_kpis.count()}")
print(f"Processing Time: {process_time:.3f}s")

os.makedirs("output/gold", exist_ok=True)
gold_kpis.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/gold/")

gold_pd = gold_kpis.limit(10).toPandas()
print("\nTop 10 Product KPIs:")
print(gold_pd.to_markdown(index=False))

print(f"\n=== BUSINESS INSIGHTS ===")
avg_revenue = gold_pd['total_revenue'].mean()
top_revenue = gold_pd['total_revenue'].max()
min_revenue = gold_pd['total_revenue'].min()
print(f"Average product revenue: ${avg_revenue:.2f}")
print(f"Top revenue product: ${top_revenue:.2f}")
print(f"Revenue range: ${min_revenue:.2f} - ${top_revenue:.2f}")

spark.stop()

print("\nGold layer completed! Check output/gold/ for results.")
