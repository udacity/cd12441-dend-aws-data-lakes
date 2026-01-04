"""
Exercise 3 Solution: Standalone Spark Medallion Pipeline
Complete implementation of Bronze → Silver → Gold data processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum as spark_sum, count, date_trunc, desc, isnan, when, max as spark_max
import pandas as pd
import time
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CloudMartMedallion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=== EXERCISE 3: MEDALLION PIPELINE PROCESSING ===\n")

# Create output directories
os.makedirs("output/silver", exist_ok=True)
os.makedirs("output/gold", exist_ok=True)

# BRONZE LAYER: Raw Data Ingestion
print("BRONZE LAYER: Loading Raw Data")
print("-" * 40)

start_time = time.time()
# Load structured orders data
bronze_orders = spark.read.parquet("data/orders.parquet")

# Load unstructured clickstream data
bronze_clicks = spark.read.json("data/clickstream.json")

bronze_load_time = time.time() - start_time

# Display schemas and counts
print("Bronze Orders Schema:")
bronze_orders.printSchema()
print(f"Bronze Orders Count: {bronze_orders.count()}")

print("\nBronze Clickstream Schema:")
bronze_clicks.printSchema()
print(f"Bronze Clickstream Count: {bronze_clicks.count()}")

print(f"Bronze Load Time: {bronze_load_time:.3f}s\n")

# SILVER LAYER: Data Cleaning and Enrichment
print("SILVER LAYER: Data Cleaning and Enrichment")
print("-" * 40)

start_time = time.time()

# Clean orders data
silver_orders = bronze_orders.filter(
    col("order_value").isNotNull() & 
    col("user_id").isNotNull() &
    (col("order_value") > 0)
).withColumn("order_date", date_trunc("day", col("order_timestamp")))

# Flatten clickstream events
silver_events = bronze_clicks.select(
    col("user_id"),
    explode(col("events.actions")).alias("action"),
    col("events.page").alias("page")
).filter(col("user_id").isNotNull())

# Join orders with events
silver = silver_orders.join(silver_events, "user_id", "left_outer") \
                     .dropDuplicates(["order_id"])

silver_process_time = time.time() - start_time

print(f"Silver Orders Count: {silver_orders.count()}")
print(f"Silver Events Count: {silver_events.count()}")
print(f"Silver Joined Count: {silver.count()}")
print(f"Silver Processing Time: {silver_process_time:.3f}s")

# Show sample silver data
print("\nSample Silver Data:")
silver.select("order_id", "user_id", "product_id", "order_value", "action", "order_date").show(5)

# GOLD LAYER: Business KPIs and Aggregations
print("\nGOLD LAYER: Business KPIs and Aggregations")
print("-" * 40)

start_time = time.time()

# Create gold KPIs
gold_kpis = silver.groupBy("product_id", "order_date") \
                  .agg(
                      spark_sum("order_value").alias("total_revenue"),
                      count("action").alias("total_events"),
                      count("order_id").alias("total_orders")
                  ) \
                  .orderBy(desc("total_revenue"))

gold_process_time = time.time() - start_time

print(f"Gold KPIs Count: {gold_kpis.count()}")
print(f"Gold Processing Time: {gold_process_time:.3f}s")

# DATA PERSISTENCE
print("\nDATA PERSISTENCE: Writing Medallion Layers")
print("-" * 40)

start_time = time.time()

# Write silver data partitioned by order_date
silver.write.mode("overwrite").partitionBy("order_date").parquet("output/silver/")

# Write gold KPIs as CSV
gold_kpis.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/gold/")

write_time = time.time() - start_time
print(f"Data Write Time: {write_time:.3f}s")

# RESULTS ANALYSIS
print("\nRESULTS ANALYSIS")
print("-" * 40)

# Convert top 10 KPIs to Pandas
gold_pd = gold_kpis.limit(10).toPandas()
print("Top 10 Product KPIs:")
print(gold_pd.to_markdown(index=False))

# Performance summary
total_time = bronze_load_time + silver_process_time + gold_process_time + write_time
print(f"\n=== PERFORMANCE SUMMARY ===")
print(f"Bronze Load Time: {bronze_load_time:.3f}s")
print(f"Silver Process Time: {silver_process_time:.3f}s") 
print(f"Gold Process Time: {gold_process_time:.3f}s")
print(f"Data Write Time: {write_time:.3f}s")
print(f"Total Pipeline Time: {total_time:.3f}s")

# Data quality metrics
orders_processed = silver_orders.count()
events_processed = silver_events.count()
joined_records = silver.count()
join_success_rate = (joined_records / orders_processed * 100) if orders_processed > 0 else 0
top_revenue = gold_pd['total_revenue'].max() if not gold_pd.empty else 0

print(f"\n=== DATA QUALITY METRICS ===")
print(f"Orders processed: {orders_processed}")
print(f"Events processed: {events_processed}")
print(f"Join success rate: {join_success_rate:.1f}%")
print(f"Top revenue product: ${top_revenue:.2f}")

# Additional insights
print(f"\n=== BUSINESS INSIGHTS ===")
if not gold_pd.empty:
    avg_revenue = gold_pd['total_revenue'].mean()
    avg_events = gold_pd['total_events'].mean()
    print(f"Average product revenue: ${avg_revenue:.2f}")
    print(f"Average events per product: {avg_events:.1f}")
    print(f"Revenue range: ${gold_pd['total_revenue'].min():.2f} - ${gold_pd['total_revenue'].max():.2f}")

spark.stop()

print("\nMedallion pipeline completed successfully!")
print("Check output/ directory for Silver and Gold layer files")

"""
Sample Data Generation Script (run separately if needed):

import pandas as pd
import json
from datetime import datetime, timedelta
import random

# Generate sample orders.parquet
orders_data = []
for i in range(1000):
    orders_data.append({
        'order_id': i + 1,
        'user_id': f'user_{random.randint(1, 100)}',
        'product_id': f'prod_{random.randint(1, 50)}',
        'order_value': round(random.uniform(10, 500), 2),
        'order_timestamp': datetime.now() - timedelta(days=random.randint(0, 30))
    })

orders_df = pd.DataFrame(orders_data)
orders_df.to_parquet('data/orders.parquet', index=False)

# Generate sample clickstream.json
clickstream_data = []
for i in range(500):
    clickstream_data.append({
        'user_id': f'user_{random.randint(1, 100)}',
        'timestamp': (datetime.now() - timedelta(hours=random.randint(0, 72))).isoformat(),
        'events': {
            'page': random.choice(['home', 'product', 'cart', 'checkout']),
            'actions': random.choices(['view', 'click', 'add_to_cart', 'purchase'], k=random.randint(1, 3))
        }
    })

with open('data/clickstream.json', 'w') as f:
    for record in clickstream_data:
        f.write(json.dumps(record) + '\n')

print("Sample data generated successfully!")
"""
