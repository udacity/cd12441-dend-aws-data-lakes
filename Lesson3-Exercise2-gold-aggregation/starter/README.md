# Exercise 2: Gold Layer Business Metrics - Student Instructions

## Objective
Transform silver layer data into aggregated business KPIs using PySpark.

## What You'll Learn
- Create daily product performance metrics
- Calculate Top 10 products by revenue
- Compute conversion rates (clicks to orders)
- Calculate customer lifetime value
- Identify churned customers

## Prerequisites
- Pre-configured Docker container with PySpark environment
- Sample data: `data/silver_orders.parquet` and `data/silver_clicks.parquet`
- Understanding of Spark aggregations (`groupBy`, `agg`)

## Step-by-Step Instructions

### Task 1: Daily Product Performance
Group by `product_id` and `order_date`, calculate revenue, order count, and average order value:
```python
gold_product_daily = silver_orders \
    .filter((col("revenue") > 0) & (col("revenue") < 10000) & (col("status") == "completed")) \
    .groupBy("product_id", "order_date") \
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("order_count"),
        avg("revenue").alias("avg_order_value")
    )
```

### Task 2: Top 10 Products (Last 30 Days)
Filter to last 30 days, group by product, order by revenue:
```python
gold_top_products = silver_orders \
    .filter((col("order_date") >= date_sub(current_date(), 30)) & (col("status") == "completed")) \
    .groupBy("product_id") \
    .agg(sum("revenue").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)
```

### Task 3: Conversion Rate
Compare unique clickers vs unique buyers:
```python
total_clicks = silver_clicks.select(countDistinct("user_id").alias("unique_clickers"))
total_orders = silver_orders.filter(col("status") == "completed") \
    .select(countDistinct("customer_id").alias("unique_buyers"))

gold_conversion = total_clicks.crossJoin(total_orders) \
    .withColumn("conversion_rate", (col("unique_buyers") / col("unique_clickers")) * 100)
```

### Task 4: Customer Lifetime Value
Group by customer, calculate total revenue and order count:
```python
gold_customer_ltv = silver_orders \
    .filter((~col("customer_id").like("test_%")) & (col("status") == "completed")) \
    .groupBy("customer_id") \
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("revenue").alias("avg_order_value")
    ) \
    .orderBy(col("total_revenue").desc())
```

### Task 5: Churned Customers
Find customers with no order in 90+ days:
```python
gold_churned = silver_orders \
    .groupBy("customer_id") \
    .agg(max("order_date").alias("last_order_date")) \
    .withColumn("days_since_last_order", datediff(current_date(), col("last_order_date"))) \
    .filter(col("days_since_last_order") > 90)
```

## Running Your Code
```bash
python gold_aggregation_exercise.py
```

## Expected Output
```
=== GOLD LAYER: BUSINESS METRICS ===

Daily Product Performance:
+----------+----------+-------------+-----------+---------------+
|product_id|order_date|total_revenue|order_count|avg_order_value|
+----------+----------+-------------+-----------+---------------+

Top 10 Products (Last 30 Days):
+----------+-------------+
|product_id|total_revenue|
+----------+-------------+

Conversion Rate:
+---------------+-------------+---------------+
|unique_clickers|unique_buyers|conversion_rate|
+---------------+-------------+---------------+

Customer Lifetime Value (Top 5):
+-----------+-------------+------------+---------------+
|customer_id|total_revenue|total_orders|avg_order_value|
+-----------+-------------+------------+---------------+

Churned Customers: <count>
```

## Common Issues
- **Empty results for Top 10**: Date filter may exclude all data — adjust the 30-day window
- **Conversion rate > 100%**: Check that you're using the right ID columns
- **No churned customers**: Data may not span 90+ days

## Success Criteria
✅ Daily product metrics calculated
✅ Top 10 products identified
✅ Conversion rate computed
✅ Customer LTV ranked
✅ Churned customers detected
