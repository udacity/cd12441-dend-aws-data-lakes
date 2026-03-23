# Exercise 3: Spark Optimization Techniques

## Overview
Learn five key Spark optimization techniques through hands-on comparison of unoptimized vs optimized pipelines.

## Learning Objectives
- Apply partitioning for parallel processing
- Use caching to avoid recomputation
- Control parallelism with coalesce/repartition
- Implement predicate pushdown
- Optimize joins with broadcast hints

## Optimization Techniques

### 1. Partitioning
Physical data organization enabling parallel processing and partition pruning.

### 2. Caching
Store DataFrames in memory when used multiple times.

### 3. Coalesce/Repartition
- Coalesce: Reduce partitions without shuffle
- Repartition: Redistribute data with shuffle

### 4. Predicate Pushdown
Filter early to reduce data movement.

### 5. Broadcast Joins
Send small tables to all executors to avoid shuffle.

## Directory Structure
```
Lesson3-Exercise3-spark-optimization/
├── README.md
├── starter/
│   └── optimization_starter.py
└── solution/
    ├── optimization_solution.py
    └── generate_data.py
```

## Instructions
1. Run `solution/generate_data.py` to create test data
2. Complete TODOs in `starter/optimization_starter.py`
3. Compare with `solution/optimization_solution.py`

## Expected Outcomes
- Understand performance impact of each optimization
- Reduce execution time by 50-80%
- Learn when to apply each technique
