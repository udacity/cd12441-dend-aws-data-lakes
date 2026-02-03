# Exercise 3: Time Travel and Schema Evolution

## Overview
Use Iceberg time travel to query historical data and evolve table schemas without downtime.

## Learning Objectives
- Query historical table versions
- View table snapshot history
- Add columns to existing tables
- Compact small files for optimization
- Compare data across versions
- Understand schema evolution patterns

## Prerequisites
- Exercise 1 & 2 completed
- Iceberg tables with multiple versions
- Understanding of versioning concepts

## Directory Structure
```
Lesson4-Exercise3-time-travel/
├── README.md                    # This file
├── starter/
│   ├── README.md               # Student instructions
│   └── time_travel_starter.py
└── solution/
    └── time_travel_solution.py
```

## Instructions
1. Start with `starter/time_travel_starter.py`
2. Read detailed instructions in `starter/README.md`
3. Complete all TODO sections for time travel queries
4. Deploy as AWS Glue 5.0 ETL job
5. Verify historical queries in Athena

## Expected Outcomes
- Current version queried
- Historical versions accessed
- Table history displayed
- Schema evolved successfully
- Files compacted
- Version comparison completed
