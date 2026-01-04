# Exercise 3: Standalone Spark Medallion Pipeline

## Overview
This exercise demonstrates the medallion architecture (Bronze → Silver → Gold) using PySpark for CloudMart's data processing. Runs locally or on Databricks/Colab without AWS dependencies.

## Directory Structure
```
Exercise3-medallion-pipeline/
├── README.md                    # This file
├── data/                        # Sample data files
│   ├── orders.parquet          # Structured orders data
│   └── clickstream.json        # Unstructured clickstream logs
├── starter/
│   └── medallion_pipeline_starter.py    # Starter code with TODO sections
└── solution/
    └── medallion_pipeline_solution.py   # Complete solution
```

## Learning Objectives
- Implement medallion architecture (Bronze → Silver → Gold)
- Process structured and unstructured data with PySpark
- Apply data quality transformations in Silver layer
- Generate business KPIs in Gold layer
- Understand partition pruning and performance optimization

## Prerequisites
- Python 3.8+
- PySpark: `pip install pyspark pandas`
- Sample data files in `data/` directory
- Local Spark environment or Databricks/Colab

## Medallion Architecture
- **Bronze**: Raw data ingestion (orders.parquet, clickstream.json)
- **Silver**: Cleaned, enriched, and joined data
- **Gold**: Aggregated business KPIs and metrics

## Instructions
1. Install dependencies: `pip install pyspark pandas`
2. Download sample data to `data/` directory
3. Start with `starter/medallion_pipeline_starter.py`
4. Complete all TODO sections for each medallion layer
5. Run locally and analyze performance metrics
6. Compare results with `solution/medallion_pipeline_solution.py`

## Expected Outcomes
- Bronze layer: Raw data loaded with schema validation
- Silver layer: Cleaned data with joins and enrichments
- Gold layer: Business KPIs partitioned by date
- Performance analysis of partition pruning
- Top 10 product revenue table output
