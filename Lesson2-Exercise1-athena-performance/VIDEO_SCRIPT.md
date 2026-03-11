# Lesson 2 - Exercise 1: Athena Query Performance Analysis

## Video Script

### Introduction (30 seconds)
"Welcome to Lesson 2, Exercise 1. In Lesson 1, we created three different data organizations in S3. Now we'll use Amazon Athena to query these datasets and measure the dramatic performance differences. You'll see firsthand why proper data organization matters for both performance and cost."

### Scene 1: Review Lesson 1 Organizations (1 minute)
"Let's quickly review what we built in Lesson 1:

**Exercise 1 - Structured Data**: We loaded orders as Parquet files into a flat S3 structure with no partitions. Fast to write, but queries must scan the entire dataset.

**Exercise 2 - Unstructured Data**: We loaded JSON clickstream events, also in a flat structure. Flexible schema, but same scanning problem.

**Exercise 3 - Organized Data**: We reorganized orders with date partitioning. Each day's data lives in its own partition. This is the key to performance."

[Show S3 console with three directory structures side by side]

### Scene 2: Athena Setup (2 minutes)
"Amazon Athena is a serverless query service that lets you analyze data in S3 using standard SQL. The key advantage? You only pay for the data you scan - $5 per terabyte.

Let's create Athena tables for our three data organizations."

[Show code editor with `athena_performance_solution.py`]

```python
def create_structured_table():
    query = f"""
    CREATE EXTERNAL TABLE orders_structured (
        order_id STRING,
        user_id STRING,
        product_id STRING,
        order_value DOUBLE,
        order_date DATE,
        status STRING
    )
    STORED AS PARQUET
    LOCATION 's3://bucket/bronze/orders/'
    """
```

"This creates a table pointing to our flat, unpartitioned data from Exercise 1."

[Show execution]

"Now for the organized table with partitions:"

```python
def create_organized_table():
    query = f"""
    CREATE EXTERNAL TABLE orders_organized (
        order_id STRING,
        ...
    )
    PARTITIONED BY (date STRING)
    STORED AS PARQUET
    LOCATION 's3://bucket/bronze/structured/orders/raw/'
    """
    execute_query(query)
    execute_query("MSCK REPAIR TABLE orders_organized")
```

"The PARTITIONED BY clause tells Athena about our date partitions. MSCK REPAIR TABLE discovers all existing partitions in S3."

### Scene 3: Benchmark 1 - Full Table Scan (2 minutes)
"Let's run our first benchmark: a full table scan that counts orders over $100."

[Show query execution]

```sql
SELECT COUNT(*) FROM orders_structured WHERE order_value > 100
```

"Structured table: 2.34 seconds, scanned 1.2 MB"

```sql
SELECT COUNT(*) FROM orders_organized WHERE order_value > 100
```

"Organized table: 2.41 seconds, scanned 1.2 MB"

[Highlight results side by side]

"Notice: NO performance difference. Why? Because we're scanning the entire dataset anyway. Partitioning doesn't help when you need all the data. In fact, there's a tiny overhead from partition metadata."

### Scene 4: Benchmark 2 - Single Date Filter (3 minutes)
"Now let's query a single day's data - January 15th. This is where partitioning shines."

[Show query execution]

```sql
SELECT * FROM orders_structured WHERE order_date = DATE '2025-01-15'
```

"Structured table: 2.18 seconds, scanned 1.2 MB - the ENTIRE dataset"

[Pause for emphasis]

"Athena had to scan every single file to find January 15th orders. Now watch what happens with partitions:"

```sql
SELECT * FROM orders_organized WHERE date = '2025-01-15'
```

"Organized table: 0.23 seconds, scanned 0.003 MB - just ONE partition"

[Show dramatic comparison]

"9.5 times faster! And we scanned 400 times less data. This is partition pruning in action. Athena looked at the WHERE clause, saw date = '2025-01-15', and only read that partition. It completely skipped the other 364 partitions."

[Show S3 console highlighting single partition being accessed]

### Scene 5: Cost Analysis (2 minutes)
"Let's talk about cost. Athena charges $5 per terabyte scanned.

Without partitions:
- 1,000 queries × 1.2 MB = 1.2 GB scanned
- Cost: $0.006

With partitions:
- 1,000 queries × 0.003 MB = 3 MB scanned  
- Cost: $0.000015

That's 99.75% cost savings!"

[Show cost calculation on screen]

"At scale, this matters. If you're running 1 million queries per month on a 100 GB dataset:
- Without partitions: $500/month
- With partitions: $5/month

Same queries, same data, 99% cost reduction."

### Scene 6: Benchmark 3 - Date Range (2 minutes)
"One more benchmark: a date range query for 6 days."

```sql
SELECT * FROM orders_structured 
WHERE order_date BETWEEN DATE '2025-01-15' AND DATE '2025-01-20'
```

"Structured: 2.31 seconds, 1.2 MB scanned"

```sql
SELECT * FROM orders_organized 
WHERE date BETWEEN '2025-01-15' AND '2025-01-20'
```

"Organized: 0.34 seconds, 0.018 MB scanned"

"6.8 times faster, 67 times less data. Athena only read 6 partitions instead of the entire dataset."

### Scene 7: When to Partition (2 minutes)
"So when should you partition? Here are the guidelines:

✅ **DO partition when:**
- You have time-series data with date filters
- Dataset is larger than 1 GB
- Most queries filter by the partition key
- You want to optimize costs

❌ **DON'T partition when:**
- Dataset is small (under 100 MB)
- Most queries are full table scans
- Partition key isn't used in WHERE clauses
- You'd create too many tiny partitions"

[Show decision tree diagram]

"The sweet spot: 128 MB to 1 GB per partition. Too small creates metadata overhead. Too large reduces pruning benefits."

### Scene 8: Key Takeaways (1 minute)
"Let's recap what we learned:

1. **Partition pruning is critical** - 9-10x speedup for filtered queries
2. **Cost optimization** - 99% reduction in data scanned
3. **Query patterns matter** - No benefit for full table scans
4. **Lesson 1 pays off** - The organization work in Exercise 3 enables these gains

This is why we spent time in Lesson 1 organizing our bronze layer. It's not just about tidiness - it's about performance and cost at scale."

### Conclusion (30 seconds)
"In the next exercise, we'll build on this foundation with Change Data Capture and incremental processing using Glue bookmarks. We'll leverage these partitions to process only new data, not the entire dataset every time.

Try the exercise yourself. Create the tables, run the benchmarks, and see the performance difference firsthand. The starter code has TODOs to guide you through each step."

[Show final summary table with all benchmark results]

---

**Total Duration**: ~15 minutes
**Key Visual Elements**:
- Side-by-side S3 directory structures
- Query execution with timing overlays
- Cost calculation animations
- Performance comparison charts
- Decision tree for partitioning strategy

**Hands-on Elements**:
- Students create Athena tables
- Execute benchmark queries
- Analyze query execution statistics
- Calculate cost implications
