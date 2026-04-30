"""
Exercise 3 Starter: Spark Optimization Techniques

TODO: Apply five optimization techniques to improve pipeline performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, broadcast
import os
import time

spark = SparkSession.builder \
    .appName("SparkOptimization") \
    .remote("sc://localhost:15002") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== SPARK OPTIMIZATION EXERCISE ===\n")

start_time = time.time()

# Read data
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
orders = spark.read.parquet(os.path.join(DATA_DIR, "orders_large.parquet"))
customers = spark.read.parquet(os.path.join(DATA_DIR, "customers.parquet"))
products = spark.read.parquet(os.path.join(DATA_DIR, "products.parquet"))

# TODO 1: Apply predicate pushdown - filter orders early
# Hint: Filter for completed orders with order_date >= "2025-01-01"
filtered_orders = orders  # Replace this

# TODO 2: Apply broadcast joins for small dimension tables
# Hint: Use broadcast() function for customers and products
joined = filtered_orders \
    .join(customers, "customer_id") \
    .join(products, "product_id")

# TODO 3: Cache the joined DataFrame (used multiple times below)
# Hint: Use .cache() and trigger with .count()

# Multiple aggregations on same data
result1 = joined.groupBy("customer_id").agg(sum("revenue").alias("total_revenue"))
print(f"Customer aggregation: {result1.count()} rows")

result2 = joined.groupBy("product_id").agg(count("*").alias("order_count"))
print(f"Product aggregation: {result2.count()} rows")

result3 = joined.groupBy("customer_id").agg(avg("revenue").alias("avg_revenue"))
print(f"Average revenue: {result3.count()} rows")

# TODO 4: Use coalesce to reduce output files
# Hint: Use .coalesce(4) before writing
joined.write.mode("overwrite").parquet("output/optimized")

# TODO 5: Unpersist cached DataFrame
# Hint: Use .unpersist()

end_time = time.time()
print(f"\nTotal Execution Time: {end_time - start_time:.2f}s")

spark.stop()
