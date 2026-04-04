# Spark Optimization Exercise - Starter Instructions

## Overview
Apply five Spark optimization techniques to improve pipeline performance.

## Prerequisites
- Pre-configured Docker container with PySpark environment
- Sample data in `data/`: `orders_large.parquet`, `customers.parquet`, `products.parquet`

## Your Tasks

### TODO 1: Predicate Pushdown
Filter orders early to reduce data movement.
- Filter for `status == "completed"`
- Filter for `order_date >= "2025-01-01"`

### TODO 2: Broadcast Joins
Use broadcast hints for small dimension tables.
- Broadcast `customers` table
- Broadcast `products` table

### TODO 3: Caching
Cache the joined DataFrame since it's used multiple times.
- Call `.cache()` on the DataFrame
- Trigger materialization with `.count()`

### TODO 4: Coalesce
Reduce the number of output files.
- Use `.coalesce(4)` before writing

### TODO 5: Unpersist
Free memory when done with cached DataFrame.
- Call `.unpersist()` on the cached DataFrame

## Running Your Code
```bash
python optimization_starter.py
```

## Expected Results
- Execution time should be significantly faster than without optimizations
- Output should show 4 parquet files instead of many small files
- Memory should be freed after unpersist
