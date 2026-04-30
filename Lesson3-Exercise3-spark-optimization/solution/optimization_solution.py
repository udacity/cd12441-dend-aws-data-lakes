"""
Exercise 3 Solution: Spark Optimization Techniques
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, broadcast
import time

spark = SparkSession.builder \
    .appName("SparkOptimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== SPARK OPTIMIZATION SOLUTION ===\n")

start_time = time.time()

# Read data
orders = spark.read.parquet("../starter/data/orders_large.parquet")
customers = spark.read.parquet("../starter/data/customers.parquet")
products = spark.read.parquet("../starter/data/products.parquet")

# Optimization 1: Predicate Pushdown - filter early
filtered_orders = orders.filter(
    (col("status") == "completed") & 
    (col("order_date") >= "2025-01-01")
)

# Optimization 2: Broadcast Joins - for small dimension tables
joined = filtered_orders \
    .join(broadcast(customers), "customer_id") \
    .join(broadcast(products), "product_id")

# Optimization 3: Caching - DataFrame used multiple times
joined.cache()
joined.count()  # Materialize cache

# Multiple aggregations now use cached data
result1 = joined.groupBy("customer_id").agg(sum("revenue").alias("total_revenue"))
print(f"Customer aggregation: {result1.count()} rows")

result2 = joined.groupBy("product_id").agg(count("*").alias("order_count"))
print(f"Product aggregation: {result2.count()} rows")

result3 = joined.groupBy("customer_id").agg(avg("revenue").alias("avg_revenue"))
print(f"Average revenue: {result3.count()} rows")

# Optimization 4: Coalesce - reduce output files
joined.coalesce(4).write.mode("overwrite").parquet("output/optimized")

# Optimization 5: Unpersist - free memory
joined.unpersist()

end_time = time.time()
print(f"\nTotal Execution Time: {end_time - start_time:.2f}s")
print("Applied: Predicate pushdown, broadcast joins, caching, coalesce, unpersist")

spark.stop()
