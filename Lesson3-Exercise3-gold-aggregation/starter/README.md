# Exercise 3: Gold Layer Aggregation - Student Instructions

## Objective
Create business KPIs and aggregated metrics in the Gold layer for analytics and reporting.

## What You'll Learn
- Group and aggregate data with Spark
- Calculate business metrics (revenue, counts)
- Order results for top-N analysis
- Write optimized output files
- Convert Spark DataFrames to Pandas
- Generate performance reports

## Prerequisites
- Exercise 2 completed (Silver layer exists)
- Pre-configured Docker container with PySpark environment
- Understanding of aggregation functions
- Knowledge of business metrics

## Step-by-Step Instructions

### Step 1: Load Silver Data
Read partitioned Silver layer:
```python
silver = spark.read.parquet("output/silver/")
```

### Step 2: Create Gold KPIs
Aggregate by product and date:
```python
gold_kpis = silver.groupBy("product_id", "order_date") \
                  .agg(
                      spark_sum("order_value").alias("total_revenue"),
                      count("action").alias("total_events"),
                      count("order_id").alias("total_orders")
                  ) \
                  .orderBy(desc("total_revenue"))
```

### Step 3: Write Gold Layer
Save as single CSV file:
```python
gold_kpis.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/gold/")
```

### Step 4: Generate Report
Convert to Pandas for analysis:
```python
gold_pd = gold_kpis.limit(10).toPandas()
print(gold_pd.to_markdown(index=False))
```

## Understanding Gold Layer
- **Business Focus**: Metrics for decision-making
- **Aggregated**: Pre-calculated summaries
- **Optimized**: Small, fast-to-query datasets
- **Report-Ready**: Formatted for BI tools

## Expected Output
```
=== GOLD LAYER: BUSINESS KPIs ===

Gold KPIs Count: 150
Processing Time: 1.234s

Top 10 Product KPIs:
| product_id | order_date | total_revenue | total_events | total_orders |
|------------|------------|---------------|--------------|--------------|
| prod_12    | 2024-01-15 | 15234.50      | 450          | 125          |
| prod_8     | 2024-01-15 | 12890.25      | 380          | 98           |
| prod_23    | 2024-01-16 | 11456.75      | 320          | 87           |

=== PERFORMANCE SUMMARY ===
Total Pipeline Time: 6.789s

=== BUSINESS INSIGHTS ===
Average product revenue: $8,234.50
Top revenue product: $15,234.50
Revenue range: $1,234.00 - $15,234.50
```

## Verification
Check output files:
```bash
ls -la output/gold/
cat output/gold/part-*.csv
```

## Common Issues
- **Too many output files**: Use coalesce(1)
- **Slow aggregation**: Check partition sizes
- **Memory error**: Reduce data or increase Spark memory

## Success Criteria
✅ Data aggregated by product and date  
✅ Revenue and event metrics calculated  
✅ Results ordered by revenue  
✅ CSV file generated  
✅ Top 10 report displayed
