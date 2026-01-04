"""
Exercise 3 Starter Code: Standalone Spark Medallion Pipeline
Student Name: _______________
Date: _______________

Instructions:
1. Complete the TODO sections for each medallion layer
2. Run locally with: python medallion_pipeline_starter.py
3. Analyze performance metrics and partition pruning
4. Verify output files in output/ directory
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum as spark_sum, count, date_trunc, desc, isnan, when
import pandas as pd
import time
import os

# TODO: Initialize Spark session with appropriate configuration
# Hint: Use SparkSession.builder with app name and adaptive query execution
spark = # YOUR CODE HERE

print("=== EXERCISE 3: MEDALLION PIPELINE PROCESSING ===\n")

# Create output directories
os.makedirs("output/silver", exist_ok=True)
os.makedirs("output/gold", exist_ok=True)

# BRONZE LAYER: Raw Data Ingestion
print("BRONZE LAYER: Loading Raw Data")
print("-" * 40)

start_time = time.time()
# TODO: Load structured orders data from Parquet
bronze_orders = # YOUR CODE HERE

# TODO: Load unstructured clickstream data from JSON
bronze_clicks = # YOUR CODE HERE

bronze_load_time = time.time() - start_time

# TODO: Display schemas and record counts
print("Bronze Orders Schema:")
# YOUR CODE HERE
print(f"Bronze Orders Count: {# YOUR CODE HERE}")

print("\nBronze Clickstream Schema:")
# YOUR CODE HERE
print(f"Bronze Clickstream Count: {# YOUR CODE HERE}")

print(f"Bronze Load Time: {bronze_load_time:.3f}s\n")

# SILVER LAYER: Data Cleaning and Enrichment
print("SILVER LAYER: Data Cleaning and Enrichment")
print("-" * 40)

start_time = time.time()

# TODO: Clean orders data - filter nulls and add date column
# Remove records with null order_value or user_id
# Add order_date column by truncating order_timestamp to day
silver_orders = bronze_orders.filter(# YOUR CODE HERE) \
                            .withColumn("order_date", # YOUR CODE HERE)

# TODO: Flatten clickstream events
# Extract user_id and explode the events.actions array
silver_events = bronze_clicks.select(# YOUR CODE HERE)

# TODO: Join orders with events and remove duplicates
# Left outer join on user_id, then deduplicate by order_id
silver = silver_orders.join(# YOUR CODE HERE) \
                     .dropDuplicates([# YOUR CODE HERE])

silver_process_time = time.time() - start_time

print(f"Silver Orders Count: {silver_orders.count()}")
print(f"Silver Events Count: {silver_events.count()}")
print(f"Silver Joined Count: {silver.count()}")
print(f"Silver Processing Time: {silver_process_time:.3f}s")

# TODO: Show sample silver data
print("\nSample Silver Data:")
# YOUR CODE HERE

# GOLD LAYER: Business KPIs and Aggregations
print("\nGOLD LAYER: Business KPIs and Aggregations")
print("-" * 40)

start_time = time.time()

# TODO: Create gold KPIs aggregated by product_id and order_date
# Group by product_id and order_date
# Calculate total_revenue (sum of order_value) and total_events (count of actions)
# Order by total_revenue descending
gold_kpis = silver.groupBy(# YOUR CODE HERE) \
                  .agg(# YOUR CODE HERE) \
                  .orderBy(# YOUR CODE HERE)

gold_process_time = time.time() - start_time

print(f"Gold KPIs Count: {gold_kpis.count()}")
print(f"Gold Processing Time: {gold_process_time:.3f}s")

# DATA PERSISTENCE
print("\nDATA PERSISTENCE: Writing Medallion Layers")
print("-" * 40)

start_time = time.time()

# TODO: Write silver data partitioned by order_date
silver.write.mode(# YOUR CODE HERE).partitionBy(# YOUR CODE HERE).parquet(# YOUR CODE HERE)

# TODO: Write gold KPIs as single CSV file
gold_kpis.coalesce(# YOUR CODE HERE).write.mode(# YOUR CODE HERE).option("header", "true").csv(# YOUR CODE HERE)

write_time = time.time() - start_time
print(f"Data Write Time: {write_time:.3f}s")

# RESULTS ANALYSIS
print("\nRESULTS ANALYSIS")
print("-" * 40)

# TODO: Convert top 10 KPIs to Pandas and display as markdown table
gold_pd = gold_kpis.limit(# YOUR CODE HERE).toPandas()
print("Top 10 Product KPIs:")
print(# YOUR CODE HERE)

# TODO: Performance summary
total_time = bronze_load_time + silver_process_time + gold_process_time + write_time
print(f"\n=== PERFORMANCE SUMMARY ===")
print(f"Bronze Load Time: {bronze_load_time:.3f}s")
print(f"Silver Process Time: {silver_process_time:.3f}s") 
print(f"Gold Process Time: {gold_process_time:.3f}s")
print(f"Data Write Time: {write_time:.3f}s")
print(f"Total Pipeline Time: {total_time:.3f}s")

# TODO: Data quality metrics
print(f"\n=== DATA QUALITY METRICS ===")
print(f"Orders processed: {# YOUR CODE HERE}")
print(f"Events processed: {# YOUR CODE HERE}")
print(f"Join success rate: {# YOUR CODE HERE}%")
print(f"Top revenue product: ${# YOUR CODE HERE}")

# TODO: Stop Spark session
# YOUR CODE HERE

print("\nMedallion pipeline completed successfully!")
print("Check output/ directory for Silver and Gold layer files")

"""
Testing Instructions:
1. Verify output/silver/ contains partitioned Parquet files
2. Verify output/gold/ contains aggregated CSV file
3. Check partition pruning: ls -la output/silver/order_date=*/
4. Analyze top revenue products and engagement patterns
5. Compare processing times across different data sizes
"""
