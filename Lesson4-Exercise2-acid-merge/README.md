# Exercise 2: ACID Transactions and MERGE Operations

## Overview
Implement ACID transactions using Iceberg MERGE operations for CDC processing.

## Learning Objectives
- Execute MERGE statements in Iceberg
- Handle CDC updates with UPSERT logic
- Implement ACID transaction guarantees
- Process incremental updates efficiently
- Understand MERGE performance optimization

## Prerequisites
- Exercise 1 completed (Iceberg tables created)
- Understanding of CDC patterns
- Knowledge of SQL MERGE syntax
- CDC update data in S3

## Directory Structure
```
Lesson4-Exercise2-acid-merge/
├── README.md                    # This file
├── starter/
│   ├── README.md               # Student instructions
│   └── acid_merge_starter.py
└── solution/
    └── acid_merge_solution.py
```

## Instructions
1. Start with `starter/acid_merge_starter.py`
2. Read detailed instructions in `starter/README.md`
3. Complete all TODO sections for MERGE operations
4. Deploy as AWS Glue 5.0 ETL job
5. Verify MERGE results in Athena

## Expected Outcomes
- Silver table created
- CDC updates loaded
- MERGE executed successfully
- Records updated and inserted
- ACID guarantees maintained
