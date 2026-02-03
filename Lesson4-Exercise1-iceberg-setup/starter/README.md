# Exercise 1: Iceberg Table Setup with S3 Tables - Student Instructions

## Objective
Configure AWS Glue 5.0 with Apache Iceberg and create S3 Tables for ACID transaction support.

## What You'll Learn
- Configure Spark for Iceberg integration
- Create Iceberg tables with partitioning
- Load data into Iceberg format
- Understand S3 Tables architecture
- Set table properties for optimization

## Prerequisites
- Pre-configured AWS Glue 5.0 environment in controlled AWS account
- Lake Formation already configured (see setup/lake_formation_setup.md for reference)
- S3 Tables bucket pre-created
- Bronze layer data available in S3
- All permissions and IAM roles pre-configured

## Step-by-Step Instructions

### Step 1: Configure Iceberg Extensions
Set Spark configurations:
```python
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.s3tables_catalog.warehouse", "s3://your-bucket/warehouse")
```

### Step 2: Create Database
```python
spark.sql("CREATE DATABASE IF NOT EXISTS s3tables_catalog.cloudmart_db")
```

### Step 3: Create Iceberg Table
```python
spark.sql("""
CREATE TABLE IF NOT EXISTS s3tables_catalog.cloudmart_db.bronze_orders (
    order_id bigint,
    user_id string,
    product_id string,
    order_value double,
    order_date timestamp,
    ingestion_timestamp timestamp,
    data_source string
) USING iceberg 
PARTITIONED BY (days(order_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
""")
```

### Step 4: Load Data
```python
bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://cloudmart/bronze/orders/"]}
).toDF()

bronze_df = bronze_df.withColumn("ingestion_timestamp", current_timestamp()) \
                    .withColumn("data_source", lit("s3_bronze_layer"))

bronze_df.write.format("iceberg").mode("append").saveAsTable("s3tables_catalog.cloudmart_db.bronze_orders")
```

## Expected Output
```
=== ICEBERG TABLE SETUP ===
Iceberg table created
Loaded 1000 records to Iceberg table
```

## Verification
Query with Athena:
```sql
SELECT * FROM s3tables_catalog.cloudmart_db.bronze_orders LIMIT 10;
DESCRIBE EXTENDED s3tables_catalog.cloudmart_db.bronze_orders;
```

## Common Issues
- **Catalog not found**: Check Spark configuration
- **Permission denied**: Verify Lake Formation permissions
- **Table exists**: Use IF NOT EXISTS clause

## Success Criteria
✅ Iceberg extensions configured  
✅ Database created in S3 Tables catalog  
✅ Iceberg table created with partitioning  
✅ Data loaded successfully  
✅ Table queryable in Athena
