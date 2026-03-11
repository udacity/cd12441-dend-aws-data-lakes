# Exercise 2: Silver Layer Transformation

## Overview
Clean, enrich, and join Bronze layer data to create quality Silver layer with transformations.

## Learning Objectives
- Filter null and invalid data
- Add derived columns (date truncation)
- Flatten nested JSON structures with explode
- Join DataFrames in Spark
- Remove duplicates
- Write partitioned Parquet files

## Prerequisites
- Exercise 1 completed
- Understanding of Spark transformations
- Knowledge of data quality principles
- Bronze layer data available

## Directory Structure
```
Lesson3-Exercise2-silver-transformation/
├── README.md                    # This file
├── starter/
│   ├── README.md               # Student instructions
│   └── silver_transformation_starter.py
└── solution/
    └── silver_transformation_solution.py
```

## Instructions
1. Start with `starter/silver_transformation_starter.py`
2. Read the detailed instructions in `starter/README.md`
3. Complete all TODO sections for data cleaning and joins
4. Run: `python silver_transformation_starter.py`
5. Verify partitioned output in `output/silver/`

## Expected Outcomes
- Invalid data filtered out
- Date column added for partitioning
- Nested JSON flattened
- Orders joined with clickstream events
- Duplicates removed
- Data partitioned by order_date
