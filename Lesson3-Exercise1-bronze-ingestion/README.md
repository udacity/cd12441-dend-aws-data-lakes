# Exercise 1: Bronze Layer Ingestion with PySpark

## Overview
Load raw structured (Parquet) and unstructured (JSON) data into Bronze layer using PySpark.

## Learning Objectives
- Initialize PySpark sessions with configuration
- Read Parquet files with Spark
- Read JSON files with Spark
- Display schemas and sample data
- Understand Bronze layer principles in medallion architecture

## Prerequisites
- Python 3.8+
- PySpark installed: `pip install pyspark`
- Sample data files in `data/` directory:
  - `orders.parquet` (structured orders data)
  - `clickstream.json` (unstructured event logs)

## Directory Structure
```
Lesson3-Exercise1-bronze-ingestion/
├── README.md                    # This file
├── starter/
│   ├── README.md               # Student instructions
│   └── bronze_ingestion_starter.py
└── solution/
    └── bronze_ingestion_solution.py
```

## Instructions
1. Start with `starter/bronze_ingestion_starter.py`
2. Read the detailed instructions in `starter/README.md`
3. Complete all TODO sections
4. Run: `python bronze_ingestion_starter.py`
5. Verify schemas and sample data displayed

## Expected Outcomes
- Spark session initialized successfully
- Structured Parquet data loaded
- Unstructured JSON data loaded
- Schemas displayed for both datasets
- Sample records shown
