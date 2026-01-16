# Exercise 2: Bronze Layer - Unstructured Data Ingestion

## Overview
This exercise demonstrates loading unstructured data (JSON format) into the bronze layer. You'll learn about schema-on-read, nested structures, and compare it with the structured data from Exercise 1.

## Learning Objectives
- ✅ Understand schema-on-read with unstructured data
- ✅ Load JSON files with nested structures using PySpark
- ✅ Handle variable fields (not all events have same fields)
- ✅ Compare structured (Parquet) vs unstructured (JSON) approaches
- ✅ Observe schema inference at read time

## Prerequisites
- Exercise 1 completed (structured data in bronze)
- Docker container running
- Environment variable set: `export BUCKET_NAME="your-bucket-name"`
- Sample data: `clickstream.json` in `/workspace/data/`
- Python with pandas, pyarrow, boto3 installed

## Concepts Covered

### Schema-on-Read (Unstructured Data)
- **Definition**: Schema is inferred when data is read, not enforced when written
- **Format**: JSON (flexible, nested, variable fields)
- **Benefits**: Flexibility for evolving schemas, nested structures
- **Trade-off**: Schema inference overhead, potential quality issues

### JSON Characteristics
- **Nested structures**: Objects within objects (metadata.browser)
- **Variable fields**: Not all records have same fields (product_id optional)
- **Self-describing**: Field names included in data
- **Human-readable**: Easy to inspect and debug

## Running the Exercise

### Step 1: Verify Prerequisites
```bash
# Inside container
cd /workspace/exercises
python preflight-check.py
```

### Step 2: Run Exercise 2
```bash
python exercise-2-bronze-unstructured.py
```

### Expected Output
```
======================================================================
EXERCISE 2: BRONZE LAYER - UNSTRUCTURED DATA INGESTION
======================================================================

[Step 1] Initializing Spark Session...
✓ Spark session initialized

[Step 2] Loading unstructured data (clickstream.json)...
  Source: /workspace/data/clickstream.json
  Format: JSON Lines (one JSON object per line)
✓ Data loaded in 1.23 seconds
  Events: 10,000
  Columns: 7

[Step 3] Examining Schema (Schema-on-Read)...
  JSON schema is INFERRED at read time - flexible structure:
root
 |-- event_id: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- metadata: struct (nullable = true)
 |    |-- browser: string (nullable = true)
 |    |-- device: string (nullable = true)
 |    |-- referrer: string (nullable = true)
 |-- page_url: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- session_id: string (nullable = true)
 |-- timestamp: string (nullable = true)
 |-- user_id: string (nullable = true)

[Step 4] Preview Sample Data (Nested JSON)...
[Shows sample events with nested metadata]

[Step 5] Exploring Nested Structure...
  JSON contains nested 'metadata' object:
  Accessing nested fields:
[Shows browser, device, referrer extracted from metadata]

[Step 6] Schema Flexibility - Variable Fields...
  Not all events have 'product_id' (only product-related events):

  Events with product_id: 6,012 (60.1%)
  Events without product_id: 3,988 (39.9%)

  ✓ Schema-on-read handles variable fields gracefully (nulls for missing)

[Step 7] Writing to S3 Bronze Layer...
  Destination: s3a://lakehouse-lesson1-yourname-123456789/bronze/clickstream/
  Strategy: Append-only (raw data preservation)
  Format: Parquet (for efficient querying, but preserving JSON structure)
✓ Data written to S3 in 4.56 seconds

[Step 8] Verifying S3 Bronze Layer...
✓ Verification successful
  Events in S3: 10,000
  Match: ✓

[Step 9] Comparison: Structured vs Unstructured...

  📊 STRUCTURED DATA (Orders - Parquet):
     • Schema: Explicit, pre-defined
     • Load time: Fast (columnar format)
     • Rows: 10,000
     • Columns: 6 (flat structure)
     • Flexibility: Low (rigid schema)

  📊 UNSTRUCTURED DATA (Clickstream - JSON):
     • Schema: Inferred at read time
     • Load time: 1.23s (schema inference overhead)
     • Events: 10,000
     • Columns: 7 (nested structure)
     • Flexibility: High (variable fields, nested objects)

======================================================================
EXERCISE 2 SUMMARY
======================================================================

📊 Unstructured Data (JSON):
   • Schema: Inferred at read time (schema-on-read)
   • Load time: 1.23s
   • Write time: 4.56s
   • Total events: 10,000
   • Nested structure: metadata object with browser/device/referrer
   • Variable fields: product_id present in 60.1% of events

🗂️  Bronze Layer Characteristics:
   • Raw data preserved as-is
   • Flexible schema (handles nested + variable fields)
   • Append-only strategy
   • Location: s3a://lakehouse-lesson1-yourname-123456789/bronze/clickstream/

✅ Key Takeaway:
   Unstructured data (JSON) uses schema-on-read - schema is inferred
   when reading, not enforced when writing. This enables flexibility
   for nested structures and variable fields, but requires careful
   handling of schema evolution and data quality.

======================================================================
Next: Exercise 3 - Silver Layer (Clean & Transform Both Data Types)
======================================================================
```

## What You Learned

### 1. Schema-on-Read Benefits
- **Flexibility**: Handles nested objects (metadata.browser)
- **Variable fields**: Some events have product_id, others don't
- **No upfront schema**: Just store JSON, infer schema when reading

### 2. JSON Characteristics
- **Nested structures**: `metadata` contains browser/device/referrer
- **Self-describing**: Field names in the data itself
- **Slower than Parquet**: Schema inference adds overhead (~0.45s vs 0.12s)

### 3. Comparison with Structured Data
| Aspect | Structured (Parquet) | Unstructured (JSON) |
|--------|---------------------|---------------------|
| Schema | Explicit, pre-defined | Inferred at read |
| Load time | 0.12s | 0.45s |
| Structure | Flat (6 columns) | Nested (7 columns) |
| Flexibility | Low | High |
| Variable fields | Not supported | Supported (nulls) |

## Verification

Check your S3 bucket:
```bash
aws s3 ls s3://lakehouse-lesson1-yourname-123456789/bronze/clickstream/
```

You should see Parquet files (JSON converted to Parquet for efficient storage).

## Common Issues

| Issue | Solution |
|-------|----------|
| `clickstream.json not found` | Run `cd /workspace/data && python generate_data.py` |
| `Schema inference slow` | Normal for JSON - Spark must scan file to infer types |
| `Nested fields not showing` | Use `col("metadata.browser")` syntax to access |
| `Comparison fails` | Run Exercise 1 first to create orders data |

## Discussion Questions

1. **Why is JSON slower to load than Parquet?**
   - Must parse text and infer schema from data
   - Text format vs binary columnar format
   - No pre-defined types to optimize

2. **How does schema-on-read handle missing fields?**
   - Infers nullable types
   - Missing fields become null values
   - Flexible but requires null handling in queries

3. **When would you choose JSON over Parquet?**
   - Source data is already JSON (APIs, logs)
   - Schema evolves frequently
   - Need human-readable format for debugging

## Next Steps

Proceed to **Exercise 3: Silver Layer** where you'll:
- Clean both structured (orders) and unstructured (clickstream) data
- Flatten nested JSON structures
- Join orders with clickstream events
- Create production-ready datasets

---

**Time to Complete**: 5-10 minutes  
**Difficulty**: Beginner  
**Prerequisites**: Exercise 1 completed
