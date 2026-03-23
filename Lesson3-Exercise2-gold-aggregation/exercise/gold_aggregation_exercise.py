"""
Exercise 2: Gold Layer Business Metrics with PySpark

OBJECTIVE: Transform silver layer data into aggregated business KPIs

TASKS:
1. Load silver layer orders and clickstream data
2. Create daily product performance metrics (revenue, order count, avg order value)
3. Calculate Top 10 products by revenue (last 30 days)
4. Compute conversion rate (clicks to orders)
5. Calculate customer lifetime value
6. Identify churned customers (no order in 90+ days)

HINTS:
- Use groupBy() for aggregations
- Use agg() with sum(), count(), avg(), countDistinct()
- Use window functions or joins for conversion rate
- Use datediff() and current_date() for churn detection
- Apply business filters: revenue > 0, exclude test users, status = 'completed'
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

# Load silver data (assume these exist from previous exercises)
silver_orders = spark.read.parquet("data/silver_orders.parquet")
silver_clicks = spark.read.parquet("data/silver_clicks.parquet")

# TODO: Task 1 - Daily Product Performance
# Group by product_id and order_date
# Calculate: total_revenue (sum), order_count (count), avg_order_value (avg)
# Filter: revenue > 0, revenue < 10000, status = 'completed'

gold_product_daily = None  # YOUR CODE HERE

# TODO: Task 2 - Top 10 Products (Last 30 Days)
# Filter to last 30 days using date_sub(current_date(), 30)
# Group by product_id, sum revenue
# Order by revenue desc, limit 10

gold_top_products = None  # YOUR CODE HERE

# TODO: Task 3 - Conversion Rate
# Count distinct user_ids who clicked (from silver_clicks)
# Count distinct customer_ids who ordered (from silver_orders, status = 'completed')
# Calculate conversion_rate = (unique_buyers / unique_clickers) * 100

gold_conversion = None  # YOUR CODE HERE

# TODO: Task 4 - Customer Lifetime Value
# Group by customer_id
# Calculate: total_revenue, total_orders, avg_order_value
# Filter: exclude test users (customer_id NOT LIKE 'test_%'), status = 'completed'
# Order by total_revenue desc

gold_customer_ltv = None  # YOUR CODE HERE

# TODO: Task 5 - Churned Customers
# Find max(order_date) per customer
# Calculate days_since_last_order = datediff(current_date(), max_order_date)
# Filter: days_since_last_order > 90

gold_churned = None  # YOUR CODE HERE

agg_time = time.time() - start_time

# Display results
if gold_product_daily:
    print("Daily Product Performance:")
    gold_product_daily.show(5)

if gold_top_products:
    print("\nTop 10 Products (Last 30 Days):")
    gold_top_products.show(10)

if gold_conversion:
    print("\nConversion Rate:")
    gold_conversion.show()

if gold_customer_ltv:
    print("\nCustomer Lifetime Value (Top 5):")
    gold_customer_ltv.show(5)

if gold_churned:
    print(f"\nChurned Customers: {gold_churned.count()}")
    gold_churned.show(5)

print(f"\nGold Aggregation Time: {agg_time:.3f}s")

# Optional: Save gold tables
# YOUR CODE HERE

spark.stop()
