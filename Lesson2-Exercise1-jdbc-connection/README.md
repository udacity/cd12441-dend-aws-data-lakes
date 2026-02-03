# Exercise 1: JDBC Connection and Full Table Load

## Overview
Learn to connect AWS Glue to PostgreSQL RDS using JDBC and perform a full table load to S3 bronze layer.

## Learning Objectives
- Configure JDBC connections in AWS Glue
- Read data from PostgreSQL using dynamic frames
- Write data to S3 in Parquet format
- Understand full table load patterns

## Prerequisites
- AWS Glue development environment
- PostgreSQL RDS instance with CloudMart orders table
- S3 bucket: `s3://cloudmart/bronze/orders/`
- JDBC driver uploaded to S3

## Instructions
1. Complete `starter/jdbc_connection_starter.py`
2. Configure JDBC connection parameters
3. Load full orders table from PostgreSQL
4. Write to S3 bronze layer as Parquet
5. Verify data with Athena

## Expected Outcomes
- Successful JDBC connection to PostgreSQL
- Full table loaded to S3 bronze layer
- Parquet files created in S3
