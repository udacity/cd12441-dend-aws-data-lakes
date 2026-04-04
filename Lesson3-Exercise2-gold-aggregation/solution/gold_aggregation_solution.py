"""
Exercise 2 Solution: Gold Layer Business Metrics with PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, count, avg, max, datediff, current_date, date_sub, when, round, lit, desc
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("GoldAggregation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== GOLD LAYER: BUSINESS METRICS ===\n")

start_time = time.time()

# Load silver data
silver_orders = spark.read.parquet("data/silver_orders.parquet")
silver_clicks = spark.read.parquet("data/silver_clicks.parquet")

# Task 1: Daily Product Performance
gold_product_daily = silver_orders \
    .filter((col("revenue") > 0) & (col("revenue") < 10000) & (col("status") == "completed")) \
    .groupBy("product_id", "order_date") \
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("order_count"),
        avg("revenue").alias("avg_order_value")
    )

# Task 2: Top 10 Products (Last 30 Days)
gold_top_products = silver_orders \
    .filter((col("order_date") >= date_sub(current_date(), 30)) & (col("status") == "completed")) \
    .groupBy("product_id") \
    .agg(sum("revenue").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)

# Task 3: Conversion Rate
total_clicks = silver_clicks.select(countDistinct("user_id").alias("unique_clickers"))
total_orders = silver_orders.filter(col("status") == "completed").select(countDistinct("customer_id").alias("unique_buyers"))

gold_conversion = total_clicks.crossJoin(total_orders) \
    .withColumn("conversion_rate", (col("unique_buyers") / col("unique_clickers")) * 100)

# Task 4: Customer Lifetime Value
gold_customer_ltv = silver_orders \
    .filter((~col("customer_id").like("test_%")) & (col("status") == "completed")) \
    .groupBy("customer_id") \
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("revenue").alias("avg_order_value")
    ) \
    .orderBy(col("total_revenue").desc())

# Task 5: Churned Customers
gold_churned = silver_orders \
    .groupBy("customer_id") \
    .agg(max("order_date").alias("last_order_date")) \
    .withColumn("days_since_last_order", datediff(current_date(), col("last_order_date"))) \
    .filter(col("days_since_last_order") > 90)

agg_time = time.time() - start_time

# Display results
print("Daily Product Performance:")
gold_product_daily.show(5)

print("\nTop 10 Products (Last 30 Days):")
gold_top_products.show(10)

print("\nConversion Rate:")
gold_conversion.show()

print("\nCustomer Lifetime Value (Top 5):")
gold_customer_ltv.show(5)

print(f"\nChurned Customers: {gold_churned.count()}")
gold_churned.show(5)

print(f"\nGold Aggregation Time: {agg_time:.3f}s")

# Optional: Save gold tables
gold_product_daily.write.mode("overwrite").parquet("data/gold_product_daily.parquet")
gold_top_products.write.mode("overwrite").parquet("data/gold_top_products.parquet")
gold_customer_ltv.write.mode("overwrite").parquet("data/gold_customer_ltv.parquet")
gold_churned.write.mode("overwrite").parquet("data/gold_churned.parquet")

print("\nGold tables saved successfully!")

spark.stop()
