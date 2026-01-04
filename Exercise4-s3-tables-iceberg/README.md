# Exercise 4: S3 Tables with Iceberg and Time Travel

## Overview
This exercise demonstrates AWS Glue 5.0 integration with S3 Tables using Apache Iceberg for ACID transactions, schema evolution, and time travel queries on CloudMart's data.

## Directory Structure
```
Exercise4-s3-tables-iceberg/
├── README.md                    # This file
├── setup/
│   └── lake_formation_setup.md # Lake Formation configuration guide
├── starter/
│   └── s3_tables_starter.py    # Starter code with TODO sections
└── solution/
    └── s3_tables_solution.py   # Complete solution
```

## Learning Objectives
- Configure AWS Glue 5.0 with S3 Tables integration
- Implement ACID transactions using Apache Iceberg
- Perform MERGE operations for CDC processing
- Execute time travel queries for historical data analysis
- Handle schema evolution in production environments

## Prerequisites
- AWS Glue 5.0 (Spark 3.5, 10+ DPU)
- S3 Tables bucket created in Lake Formation
- Lake Formation permissions configured
- Bronze layer data in S3 from previous exercises
- AWS Glue IAM role with S3 Tables permissions

## S3 Tables Features
- **ACID Transactions**: Consistent reads and writes
- **Time Travel**: Query historical table versions
- **Schema Evolution**: Add/modify columns without downtime
- **MERGE Operations**: Efficient CDC processing
- **Partition Evolution**: Change partitioning schemes

## Setup Requirements
1. Enable S3 Tables in Lake Formation console
2. Create S3 Tables bucket for Iceberg metadata
3. Configure Glue IAM role with Lake Formation permissions
4. Enable external engine integration in Lake Formation

## Instructions
1. Complete Lake Formation setup using `setup/lake_formation_setup.md`
2. Start with `starter/s3_tables_starter.py`
3. Complete TODO sections for Iceberg configuration
4. Deploy as AWS Glue 5.0 ETL job
5. Test ACID operations and time travel queries
6. Verify results in Athena and S3 Tables console

## Expected Outcomes
- Iceberg tables with ACID transaction support
- Successful MERGE operations for CDC processing
- Time travel queries returning historical data
- Schema evolution without data migration
- Optimized query performance with partition pruning
