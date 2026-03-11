# Exercise 2: Gold Layer Business Metrics

## Learning Objectives
- Transform silver data into aggregated business KPIs
- Apply the Top-K pattern for ranking analysis
- Calculate conversion rates using funnel analysis
- Compute customer lifetime value
- Identify churned entities using recency patterns
- Apply business filters at the gold layer

## Concepts Covered
1. **Aggregation Patterns**: groupBy + agg with sum, count, avg, countDistinct
2. **Top-K Pattern**: Filter → Aggregate → Sort → Limit
3. **Conversion Rate**: Left join pattern for funnel analysis
4. **Lifetime Metrics**: Full history aggregation without time filters
5. **Churn Detection**: Recency-based business rules
6. **Gold Layer Filtering**: Business-specific data quality rules

## Exercise Structure
- `exercise/gold_aggregation_exercise.py` - Starter code with TODOs
- `solution/gold_aggregation_solution.py` - Complete implementation
- `data/` - Sample silver layer data (orders and clickstream)

## Tasks
1. Create daily product performance metrics
2. Find top 10 products by revenue (last 30 days)
3. Calculate conversion rate from clicks to orders
4. Compute customer lifetime value
5. Identify churned customers (90+ days inactive)

## Key Patterns Demonstrated
- **Daily Product Performance**: Standard aggregation with business filters
- **Top Products**: Top-K pattern with time window
- **Conversion Rate**: Funnel analysis with distinct counts
- **Customer LTV**: Lifetime aggregation excluding test data
- **Churn**: Recency calculation with threshold

## Running the Exercise
```bash
# Run the exercise (fill in TODOs first)
spark-submit exercise/gold_aggregation_exercise.py

# Run the solution
spark-submit solution/gold_aggregation_solution.py
```

## Expected Output
- Daily product metrics with revenue, count, and averages
- Top 10 products ranked by revenue
- Overall conversion rate percentage
- Customer LTV sorted by total revenue
- Count and list of churned customers
