# Exercise 3: Partitioning and Optimization

## Overview
Optimize CDC pipeline with date partitioning and file size optimization for efficient querying and storage.

## Learning Objectives
- Implement date-based partitioning
- Optimize file sizes with coalesce
- Understand partition pruning benefits
- Query partitioned data with Athena
- Compare performance with/without partitioning

## Prerequisites
- Exercise 1 & 2 completed
- Understanding of Hive-style partitioning
- Basic knowledge of query optimization

## Instructions
1. Complete `starter/partitioning_optimization_starter.py`
2. Add order_date partition column
3. Use coalesce to optimize file sizes
4. Write with partitionBy
5. Query with Athena and compare performance

## Expected Outcomes
- Data partitioned by order_date in S3
- Optimized file sizes (not too many small files)
- Faster queries with partition pruning
- Efficient storage layout
