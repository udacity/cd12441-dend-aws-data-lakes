# AWS Data Lakes Course Exercises

This repository contains hands-on exercises for the AWS Data Lakes course, demonstrating practical data engineering concepts from bronze layer ingestion to advanced ACID transactions with S3 Tables.

## Course Overview

Students will learn to build and manage data lakes on AWS through progressive exercises covering:
- Bronze layer data ingestion (structured and unstructured)
- Data organization and partitioning strategies
- Change Data Capture (CDC) patterns
- Medallion architecture processing (Bronze → Silver → Gold)
- Advanced ACID transactions and time travel with S3 Tables

## Environment Requirements

### Core Dependencies
```bash
pip install pandas pyarrow boto3 pyspark
```

### AWS Services Required
- **Amazon S3**: Data lake storage
- **AWS Glue**: ETL jobs and data catalog
- **Amazon RDS PostgreSQL**: Source database for CDC
- **AWS Lake Formation**: S3 Tables and governance
- **Amazon Athena**: Query engine for validation

### Local Development Setup
```bash
# Clone repository
git clone <repository-url>
cd cd12441-dend-aws-data-lakes

# Install Python dependencies
pip install -r requirements.txt
```

### AWS Credentials
The exercises load AWS credentials from `/workspace/.env` (read by [load_env.py](load_env.py)).
In the Udacity workspace, get your `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
`AWS_SESSION_TOKEN` from the **Cloud Resources** tab and paste them into `/workspace/.env`.
See the course **Setup** page for the full template and re-paste instructions when the
session token expires. Exercises that run purely locally (Lesson 2 Exercise 2 and all of
Lesson 3) do not require credentials.

### Additional Requirements
- **Python 3.8+**: Required for all exercises
- **Java 8/11**: Required for PySpark local execution

## Exercise Structure

### Lesson 1: Bronze Layer Fundamentals
```
Lesson1-Exercise1-structured/           # Structured Parquet ingestion
├── starter/structured_ingestion_starter.py
├── solution/structured_ingestion_solution.py
└── README.md

Lesson1-Exercise2-unstructured/         # Unstructured JSON ingestion
├── starter/unstructured_ingestion_starter.py
├── solution/unstructured_ingestion_solution.py
└── README.md

Lesson1-Exercise3-organization/         # Date partitioning & organization
├── starter/bronze_organization_starter.py
├── solution/bronze_organization_solution.py
└── README.md
```

### Advanced Exercises
```
Exercise2-cdc-ingestion-bronze/         # CDC with AWS Glue
├── starter/cdc_ingestion_starter.py
├── solution/cdc_ingestion_solution.py
└── README.md

Exercise3-medallion-pipeline/           # Bronze → Silver → Gold
├── starter/medallion_pipeline_starter.py
├── solution/medallion_pipeline_solution.py
├── data/                               # Sample data
└── README.md

Exercise4-s3-tables-iceberg/            # ACID transactions
├── starter/s3_tables_starter.py
├── solution/s3_tables_solution.py
├── setup/lake_formation_setup.md
└── README.md
```

## Exercise Progression

### Lesson 1: Bronze Layer Fundamentals (60 minutes)

#### Exercise 1: Structured Data Ingestion (15-20 min)
**Technology**: Python, pandas, pyarrow, boto3  
**Concepts**: Schema-on-write, Parquet format, metadata tracking  
**Prerequisites**: AWS S3 access, Python environment

#### Exercise 2: Unstructured Data Ingestion (20-25 min)
**Technology**: Python, pandas, boto3  
**Concepts**: Schema-on-read, JSON flattening, variable fields  
**Prerequisites**: Completion of Exercise 1

#### Exercise 3: Bronze Layer Organization (25-30 min)
**Technology**: Python, pandas, boto3  
**Concepts**: Date partitioning, hierarchical structure, query optimization  
**Prerequisites**: Completion of Exercises 1 & 2

### Exercise 2: CDC Ingestion to Bronze Layer (60 minutes)
**Technology**: AWS Glue, PySpark, PostgreSQL  
**Concepts**: Change Data Capture, incremental loading, bookmarks  
**Prerequisites**: AWS Glue setup, RDS PostgreSQL instance

### Exercise 3: Medallion Pipeline Processing (75 minutes)
**Technology**: PySpark (standalone)  
**Concepts**: Bronze → Silver → Gold architecture, data quality  
**Prerequisites**: Local PySpark installation

### Exercise 4: S3 Tables with Iceberg (90 minutes)
**Technology**: AWS Glue 5.0, Apache Iceberg, Lake Formation  
**Concepts**: ACID transactions, time travel, schema evolution  
**Prerequisites**: Lake Formation setup, S3 Tables enabled

## Getting Started

1. **Setup Environment**: Install dependencies and configure AWS credentials
2. **Start with Lesson 1**: Complete all three bronze layer exercises sequentially
3. **Complete Starter Code**: Fill in TODO sections in starter files
4. **Verify Results**: Compare with solution implementations
5. **Progress to Advanced**: Move to CDC, medallion, and S3 Tables exercises

## Learning Outcomes

By completing these exercises, students will:
- Master bronze layer ingestion for structured and unstructured data
- Implement effective data organization and partitioning strategies
- Build production-ready CDC ingestion patterns
- Design scalable medallion architecture pipelines
- Leverage advanced data lake features like ACID transactions and time travel
- Gain hands-on experience with AWS data engineering services

## Support and Troubleshooting

Each exercise includes:
- Detailed setup instructions
- Step-by-step implementation guides
- Common troubleshooting tips
- Verification steps and expected outputs
- Performance optimization recommendations

For additional support, refer to individual exercise README files and AWS documentation. 


