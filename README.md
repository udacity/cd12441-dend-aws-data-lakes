# AWS Data Lakes Course Exercises

This repository contains hands-on exercises for the AWS Data Lakes course, demonstrating practical data engineering concepts from basic lake vs warehouse comparisons to advanced ACID transactions with S3 Tables.

## Course Overview

Students will learn to build and manage data lakes on AWS through four progressive exercises covering:
- Data lake flexibility vs data warehouse rigidity
- Change Data Capture (CDC) ingestion patterns
- Medallion architecture processing (Bronze → Silver → Gold)
- Advanced ACID transactions and time travel with S3 Tables

## Environment Requirements and Installation

### Core Dependencies
```bash
pip install pandas pyarrow boto3 pyspark
```

### AWS Services Required
- **AWS Glue**: ETL jobs and data catalog
- **Amazon S3**: Data lake storage
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

# Configure AWS credentials
aws configure
```

### Additional Requirements
- **Python 3.8+**: Required for all exercises
- **AWS CLI**: For resource management and deployment
- **PostgreSQL Client**: For database connectivity testing
- **Java 8/11**: Required for PySpark local execution

## Exercise Structure

```
Exercise1-structured-vs-unstructured/     # Data Lake vs Warehouse concepts
├── starter/                              # Student template with TODOs
├── solution/                             # Complete working solution
└── README.md                             # Exercise-specific instructions

Exercise2-cdc-ingestion-bronze/           # CDC ingestion to bronze layer
├── starter/
├── solution/
├── setup/                                # Configuration guides
└── README.md

Exercise3-medallion-pipeline/             # Standalone Spark processing
├── starter/
├── solution/
├── data/                                 # Sample data and generators
└── README.md

Exercise4-s3-tables-iceberg/              # Advanced ACID transactions
├── starter/
├── solution/
├── setup/                                # Lake Formation setup
└── README.md
```

## Exercise Progression

### Exercise 1: Structured vs Unstructured Processing
**Technology**: Python, pandas, boto3  
**Concepts**: Schema-on-read vs schema-on-write, data lake flexibility  
**Duration**: 45 minutes  
**Prerequisites**: Basic Python, AWS S3 access

### Exercise 2: CDC Ingestion to Bronze Layer
**Technology**: AWS Glue, PySpark, PostgreSQL  
**Concepts**: Change Data Capture, incremental loading, bookmarks  
**Duration**: 60 minutes  
**Prerequisites**: AWS Glue setup, RDS PostgreSQL instance

### Exercise 3: Medallion Pipeline Processing
**Technology**: PySpark (standalone)  
**Concepts**: Bronze → Silver → Gold architecture, data quality  
**Duration**: 75 minutes  
**Prerequisites**: Local PySpark installation

### Exercise 4: S3 Tables with Iceberg
**Technology**: AWS Glue 5.0, Apache Iceberg, Lake Formation  
**Concepts**: ACID transactions, time travel, schema evolution  
**Duration**: 90 minutes  
**Prerequisites**: Lake Formation setup, S3 Tables enabled

## Getting Started

1. **Setup Environment**: Install dependencies and configure AWS credentials
2. **Choose Exercise**: Start with Exercise 1 for foundational concepts
3. **Complete Starter Code**: Fill in TODO sections in starter files
4. **Verify Results**: Compare with solution implementations
5. **Deploy to AWS**: Test with real AWS services (Exercises 2 & 4)

## Learning Outcomes

By completing these exercises, students will:
- Understand fundamental differences between data lakes and warehouses
- Implement production-ready CDC ingestion patterns
- Build scalable medallion architecture pipelines
- Master advanced data lake features like ACID transactions and time travel
- Gain hands-on experience with AWS data engineering services

## Support and Troubleshooting

Each exercise includes:
- Detailed setup instructions
- Common troubleshooting guides
- Verification steps and expected outputs
- Performance optimization tips

For additional support, refer to individual exercise README files and AWS documentation. 


