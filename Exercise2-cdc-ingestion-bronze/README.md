# Exercise 2: CDC Ingestion to Bronze Layer

## Overview
This exercise demonstrates Change Data Capture (CDC) ingestion from PostgreSQL to S3 bronze layer using AWS Glue PySpark, implementing incremental data loading patterns.

## Directory Structure
```
Exercise2-cdc-ingestion-bronze/
├── README.md                    # This file
├── starter/
│   └── cdc_ingestion_starter.py    # Starter code with TODO sections
└── solution/
    └── cdc_ingestion_solution.py   # Complete solution
```

## Learning Objectives
- Implement CDC patterns using bookmarks
- Build incremental data pipelines with AWS Glue
- Partition data in S3 bronze layer
- Handle JDBC connections and dynamic queries
- Test CDC with database updates

## Prerequisites
- AWS Glue development environment
- PostgreSQL RDS instance with CloudMart orders table
- S3 bucket: `s3://cloudmart/bronze/orders/`
- AWS Glue JDBC connection configured
- Basic PySpark and AWS Glue knowledge

## Data Pipeline
- **Source**: PostgreSQL RDS orders table
- **Method**: Incremental CDC using `updated_at` timestamp
- **Target**: S3 bronze layer partitioned by `order_date`
- **Format**: Parquet files for efficient querying

## Instructions
1. Start with `starter/cdc_ingestion_starter.py`
2. Complete all TODO sections for CDC implementation
3. Deploy as AWS Glue ETL job with bookmark parameter
4. Test incremental loading by updating PostgreSQL data
5. Verify results using Amazon Athena queries

## Expected Outcomes
- Incremental data loading without full table scans
- Partitioned Parquet files in S3 bronze layer
- Dynamic bookmark management for CDC
- Efficient append-only data pipeline
