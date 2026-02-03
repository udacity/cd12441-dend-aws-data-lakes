# Exercise 1: Iceberg Table Setup with S3 Tables

## Overview
Configure AWS Glue 5.0 with Apache Iceberg and create S3 Tables for ACID transaction support.

## Learning Objectives
- Configure Spark for Iceberg integration
- Create Iceberg tables with partitioning
- Load data into Iceberg format
- Understand S3 Tables architecture
- Set table properties for optimization

## Prerequisites
- AWS Glue 5.0 (Spark 3.5, 10+ DPU)
- Lake Formation configured (see setup/lake_formation_setup.md)
- S3 Tables bucket created
- Bronze layer data in S3

## Directory Structure
```
Lesson4-Exercise1-iceberg-setup/
├── README.md                    # This file
├── setup/
│   └── lake_formation_setup.md # Lake Formation configuration
├── starter/
│   ├── README.md               # Student instructions
│   └── iceberg_setup_starter.py
└── solution/
    └── iceberg_setup_solution.py
```

## Instructions
1. Complete Lake Formation setup from `setup/lake_formation_setup.md`
2. Start with `starter/iceberg_setup_starter.py`
3. Read detailed instructions in `starter/README.md`
4. Complete all TODO sections
5. Deploy as AWS Glue 5.0 ETL job
6. Verify table in Athena

## Expected Outcomes
- Iceberg extensions configured
- Database created in S3 Tables catalog
- Iceberg table with partitioning
- Data loaded successfully
- Table queryable in Athena
