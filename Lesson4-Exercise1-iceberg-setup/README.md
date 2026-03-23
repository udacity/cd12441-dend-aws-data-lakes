# Exercise 1: S3 Tables Setup with Native Iceberg Support

## Overview
Create and manage Amazon S3 Tables using boto3 and query with Athena. Learn S3 Tables' built-in Iceberg support and automated optimization.

## Learning Objectives
- Create S3 table buckets using boto3
- Create namespaces and tables via API
- Understand S3 Tables vs traditional Iceberg
- Query tables with Athena
- Leverage automated maintenance features

## Prerequisites
- boto3 installed
- IAM permissions for s3tables:* actions
- Sample data in S3 (optional)

## Directory Structure
```
Lesson4-Exercise1-iceberg-setup/
├── README.md                    # This file
├── setup/
│   └── s3_tables_setup.py      # Create table bucket and namespace
├── starter/
│   ├── README.md               # Student instructions
│   └── create_table.sql        # Athena SQL to create table
└── solution/
    └── create_table.sql        # Complete SQL solution
```

## Instructions
1. Run `python setup/s3_tables_setup.py` to create table bucket
2. Open Athena console
3. Use SQL from `starter/create_table.sql` to create table
4. Load data with INSERT or COPY
5. Query and verify automated optimization

## Expected Outcomes
- S3 table bucket created with boto3
- Namespace and table created
- Data loaded successfully
- Table queryable with 3x faster performance
- Automated maintenance enabled (no manual work)
