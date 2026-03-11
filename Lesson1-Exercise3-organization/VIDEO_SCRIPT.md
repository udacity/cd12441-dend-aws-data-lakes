# Video Script: Exercise 3 - Bronze Layer Organization

## Introduction (30 seconds)

"Welcome to Exercise 3, where we bring everything together by organizing our bronze layer properly. In Exercises 1 and 2, we ingested structured and unstructured data, but we just dumped files into S3 without much organization. Now we'll learn how to structure a bronze layer for production use with proper partitioning, metadata tracking, and hierarchical organization. This is what separates a proof-of-concept from a production-ready data lake."

## Learning Objectives (20 seconds)

"By the end of this exercise, you'll understand:
- Bronze layer organization principles and best practices
- Date partitioning strategies for efficient queries
- Metadata tracking for data lineage
- How to structure different data types in a unified architecture"

## Code Walkthrough - Configuration (20 seconds)

"Our configuration is simple - just the bucket name. We're working with the same data from Exercises 1 and 2, but now we're reorganizing it into a proper structure."

## Step 1: Initialize S3 Client (15 seconds)

"Step 1 initializes our S3 client. Same as before - this is our gateway to all S3 operations."

## Step 2: Review Current Structure (1 minute)

"Step 2 shows the problem we're solving. Look at our current bronze layer structure from Exercises 1 and 2. We have orders in one directory and clickstream in another. Simple, but problematic.

What's wrong with this? Four major issues:

First, no partitioning. Every query must scan the entire file, even if you only want one day's data.

Second, no metadata. We don't know when data was ingested, where it came from, or what version of the schema we're using.

Third, no data type separation. Structured and unstructured data are mixed at the same level, making it hard to apply different processing strategies.

Fourth, no incremental load support. How do you add new data without reprocessing everything? You can't, with this structure."

## Step 3: Design Organized Structure (1 minute 30 seconds)

"Step 3 presents the solution - a hierarchical, organized bronze layer. Let's break down this structure.

At the top level, we separate by data type: structured and unstructured. This lets us apply different processing strategies to each.

Under each data type, we organize by source system: orders from PostgreSQL, clickstream from web analytics. This provides clear data lineage.

Under each source, we have a 'raw' directory. This preserves the original format for audit trails and reprocessing.

Inside raw, we partition by date. This is the key to query efficiency. Instead of scanning gigabytes, you scan only the partitions you need.

Finally, we add metadata.json files at the dataset level to track schema versions, row counts, and ingestion details.

This structure gives us five major benefits: clear data type separation, source system identification, date partitioning for efficient queries, metadata tracking for lineage, and raw format preservation for compliance."

## Step 4: Reorganize Structured Data (2 minutes)

"Now let's reorganize our orders data. Step 4 shows the implementation.

First, we list objects in the orders directory to find existing parquet files. We're using list_objects_v2, which is more efficient than the older list_objects.

We wrap this in a try-except block because the data might not exist if you haven't run Exercise 1. Good error handling is crucial in production code.

Once we find the file, we read it using get_object and load it into a pandas DataFrame. Notice we're reading from S3, not local files.

Next, we add metadata columns. These are critical for tracking:
- ingestion_timestamp: When did we load this data?
- source_system: Where did it come from? PostgreSQL in this case.
- data_type: Is this structured or unstructured?
- ingestion_date: What date do we partition by? We extract this from order_date.

Now comes the partitioning logic. We group by ingestion_date and write each date to its own partition. The partition path includes 'date=' followed by the date value. This is Hive-style partitioning, which tools like Athena and Spark recognize automatically.

For each partition, we write to a BytesIO buffer, then upload to S3. We're timing this to understand performance. The result? Multiple partitions instead of one monolithic file."

## Step 5: Reorganize Unstructured Data (1 minute 30 seconds)

"Step 5 does the same for clickstream data, but with a few differences.

We list objects in the clickstream directory and read the parquet file. Remember, we converted JSON to Parquet in Exercise 2.

The metadata columns are similar, but source_system is 'web_analytics' instead of 'postgresql'. And we extract ingestion_date from the timestamp field, not order_date.

The partitioning logic is identical - group by date, write each partition separately. This consistency across data types makes our pipeline easier to maintain.

Notice both structured and unstructured data get the same treatment: metadata columns and date partitioning. The organization strategy is consistent, even though the data types differ."

## Step 6: Create Metadata Files (1 minute 30 seconds)

"Step 6 creates metadata files for each dataset. This is often overlooked but incredibly valuable.

For orders, we create a JSON object with key information:
- Dataset name and data type for identification
- Source system for lineage tracking
- Format - Parquet in this case
- Schema version for evolution tracking
- Partitioning strategy - date-based
- Ingestion frequency - daily for orders
- Row count and column count for quality checks
- Created timestamp for audit trails

We do the same for clickstream, but note the differences: ingestion frequency is hourly instead of daily, and we track nested_fields to document the complex structure.

These metadata files serve multiple purposes: They document your data for new team members, enable automated quality checks, support schema evolution tracking, and provide audit trails for compliance.

We upload these as JSON files to S3, making them easily accessible to any tool or person exploring the data lake."

## Step 7: Demonstrate Partition Benefits (1 minute 15 seconds)

"Step 7 proves why partitioning matters. We query a single date partition and time the operation.

Look at the partition path - it includes the specific date. When we read this, S3 only retrieves that one file, not the entire dataset.

The timing shows this is fast - typically under a second. But imagine if we had a year of data. Without partitioning, we'd scan 365 days of data. With partitioning, we scan one day.

This is called partition pruning. Query engines like Athena and Spark automatically skip irrelevant partitions based on your WHERE clause. If you query 'WHERE date = 2026-01-15', only that partition is read.

The efficiency gain scales linearly. With 365 partitions, you get 365x faster queries for single-day queries. This is why partitioning is fundamental to data lake performance."

## Step 8: Compare Organized vs Unorganized (1 minute 30 seconds)

"Step 8 presents a side-by-side comparison. This really drives home why organization matters.

The unorganized structure from Exercises 1 and 2 has flat files with no partitioning. Every query is a full table scan. There's no metadata, so you don't know data lineage. There's no data type separation, making it hard to apply different processing strategies. And there's no support for incremental loads - you'd have to reprocess everything.

The organized structure has hierarchical directories with clear separation. Date partitioning enables efficient queries through partition pruning. Metadata tracking provides clear lineage and quality metrics. Data type separation allows different processing strategies. And incremental loads are simple - just add new date partitions.

This isn't just about being neat. This is about query performance, operational efficiency, and maintainability at scale."

## Summary Section (1 minute 30 seconds)

"Let's recap what we've accomplished in Exercise 3.

We reorganized our bronze layer with a hierarchical structure: data type, then source system, then raw data with date partitions. We added metadata columns to every record for tracking and lineage. We created metadata files documenting each dataset's characteristics.

The key improvements are:
1. Data type separation makes it easy to apply different processing strategies
2. Date partitioning enables partition pruning for 10x to 100x query speedups
3. Metadata tracking provides lineage, quality metrics, and audit trails
4. Incremental load support through append-only partitions
5. Clear directory structure that's self-documenting

These improvements set us up for success in the silver layer. With proper organization, our ETL pipelines will be faster, more maintainable, and more reliable.

The bronze layer is now production-ready. It preserves raw data for audit trails, organizes data for efficient access, tracks metadata for lineage, and supports incremental processing."

## Lesson 1 Complete (45 seconds)

"Congratulations! You've completed Lesson 1 on Data Lakes and Lakehouses.

Let's review what you've learned across all three exercises:

Exercise 1 taught you structured data ingestion with schema-on-write using Parquet. You learned to preserve data quality issues in bronze for downstream cleaning.

Exercise 2 taught you unstructured data ingestion with schema-on-read using JSON. You learned to handle nested structures and variable fields.

Exercise 3 taught you bronze layer organization with partitioning and metadata. You learned production-ready data lake architecture.

You now understand the bronze layer's role: preserve raw data exactly as received, organize for efficient access, track metadata for lineage, and support incremental processing.

In the next lesson, we'll move to the silver layer, where we'll clean, validate, and transform this bronze data into analysis-ready datasets. Thanks for following along!"

---

## Technical Notes for Video Production

**Total Runtime:** ~12 minutes

**Screen Recording Sections:**
1. Show full script in editor (0:00-0:30)
2. Review unorganized structure from Ex 1 & 2 (0:30-2:00)
3. Show organized structure diagram (2:00-3:30)
4. Step through orders reorganization (3:30-5:30)
5. Step through clickstream reorganization (5:30-7:00)
6. Show metadata file creation (7:00-8:30)
7. Demonstrate partition query (8:30-9:45)
8. Side-by-side comparison (9:45-11:15)
9. Show S3 console with organized structure (11:15-12:00)

**Visual Aids to Include:**
- Diagram: Unorganized vs Organized bronze layer
- Diagram: Hive-style partitioning explanation
- Diagram: Partition pruning visualization
- Animation: How partition pruning works
- S3 Console: Before and after organization
- Terminal: Script execution with timing

**Key Terms to Emphasize:**
- Hierarchical organization
- Date partitioning
- Hive-style partitioning
- Partition pruning
- Metadata tracking
- Data lineage
- Incremental loads
- Append-only strategy

**Code Highlights:**
- list_objects_v2 for finding files
- groupby for partitioning logic
- Partition path format: `date=YYYY-MM-DD`
- Metadata JSON structure
- Error handling with try-except

**Before/After Comparison to Show:**
```
BEFORE (Unorganized):
s3://bucket/
├── orders/orders.parquet (10GB, all dates)
└── clickstream/clickstream.parquet (50GB, all dates)

Query: "Get orders for 2026-01-15"
Scans: 10GB (entire file)

AFTER (Organized):
s3://bucket/
├── structured/orders/raw/
│   ├── date=2026-01-15/orders.parquet (30MB)
│   ├── date=2026-01-16/orders.parquet (30MB)
│   └── ... (365 partitions)
└── unstructured/clickstream/raw/
    ├── date=2026-01-15/clickstream.parquet (150MB)
    └── ... (365 partitions)

Query: "Get orders for 2026-01-15"
Scans: 30MB (one partition only)
Speedup: 333x faster!
```

**Performance Metrics to Highlight:**
- Partition count created
- Time to reorganize
- Single partition query time
- Efficiency gain calculation
