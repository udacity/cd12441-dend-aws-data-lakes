# Exercise 1: Structured vs Unstructured Data Processing

## Overview
This exercise demonstrates the fundamental differences between data lake and data warehouse approaches using CloudMart's e-commerce data.

## Directory Structure
```
Exercise1-structured-vs-unstructured/
├── README.md                    # This file
├── starter/
│   └── lake_vs_warehouse_starter.py    # Starter code with TODO sections
└── solution/
    └── lake_vs_warehouse_solution.py   # Complete solution
```

## Learning Objectives
- Understand schema-on-read vs schema-on-write paradigms
- Experience data lake flexibility with variable JSON structures
- Compare processing performance between approaches
- Analyze mixed structured and unstructured data

## Prerequisites
- Python 3.8+
- Libraries: pandas, pyarrow, boto3
- AWS credentials configured
- Access to `s3://cloudmart/demo/` sample data

## Data Sources
- **Structured**: `orders.parquet` - Fixed schema orders data
- **Unstructured**: `clickstream.json` - Variable schema web logs

## Instructions
1. Start with `starter/lake_vs_warehouse_starter.py`
2. Complete all TODO sections
3. Compare your results with `solution/lake_vs_warehouse_solution.py`
4. Run both approaches and analyze the differences

## Expected Outcomes
- Demonstrate lake schema flexibility vs warehouse rigidity
- Show unified analysis capabilities
- Measure performance differences
- Generate business insights from mixed data sources
