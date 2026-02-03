# Exercise 3: Gold Layer Aggregation

## Overview
Create business KPIs and aggregated metrics in Gold layer for analytics and reporting.

## Learning Objectives
- Group and aggregate data with Spark
- Calculate business metrics (revenue, events, orders)
- Order results for top-N analysis
- Write optimized CSV output
- Convert Spark DataFrames to Pandas
- Generate business insights reports

## Prerequisites
- Exercise 2 completed
- Silver layer data available in `output/silver/`
- Understanding of aggregation functions
- Knowledge of business metrics

## Directory Structure
```
Lesson3-Exercise3-gold-aggregation/
├── README.md                    # This file
├── starter/
│   ├── README.md               # Student instructions
│   └── gold_aggregation_starter.py
└── solution/
    └── gold_aggregation_solution.py
```

## Instructions
1. Start with `starter/gold_aggregation_starter.py`
2. Read the detailed instructions in `starter/README.md`
3. Complete all TODO sections for aggregations
4. Run: `python gold_aggregation_starter.py`
5. Verify CSV output in `output/gold/`
6. Review top 10 product KPIs report

## Expected Outcomes
- Data aggregated by product and date
- Revenue, events, and orders metrics calculated
- Results ordered by total revenue
- Single CSV file generated
- Top 10 products report displayed
- Business insights summary
