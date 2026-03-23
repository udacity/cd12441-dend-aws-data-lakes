-- Exercise 1: Create S3 Table with Athena and Insert Sample Data
-- Prerequisites: Run solution/complete_setup.py first to create table bucket and namespace

-- Step 1: Create silver_orders table in S3 Tables
-- TODO: Create the table using the catalog s3tablescatalog/swiftshop-analytics-tables
-- Hint: Use TBLPROPERTIES ('table_type' = 'ICEBERG')
CREATE TABLE s3tables.swiftshop.silver_orders (
    order_id STRING,
    user_id STRING,
    product_id STRING,
    order_value DOUBLE,
    order_date TIMESTAMP,
    status STRING,
    processed_at TIMESTAMP
)
-- YOUR CODE HERE - add TBLPROPERTIES

-- Step 2: Insert sample data
-- TODO: Insert sample order records
INSERT INTO s3tables.swiftshop.silver_orders VALUES
    -- YOUR CODE HERE

-- Step 3: Query the table
SELECT * FROM s3tables.swiftshop.silver_orders ORDER BY order_date;

-- Step 4: Verify data
SELECT status, COUNT(*) as order_count, SUM(order_value) as total_value
FROM s3tables.swiftshop.silver_orders
GROUP BY status
ORDER BY total_value DESC;

-- Step 5: Check table metadata
DESCRIBE EXTENDED s3tables.swiftshop.silver_orders;
