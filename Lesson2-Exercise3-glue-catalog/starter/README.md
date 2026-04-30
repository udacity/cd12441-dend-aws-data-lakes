# Lesson 2 - Exercise 3: AWS Glue Crawler (Starter Guide)

## Objective
Create AWS Glue crawler to automatically discover schemas and partitions from S3 data, making it queryable by Athena and other tools.

## Prerequisites

### AWS Credentials
AWS credentials in `/workspace/.env` (see the course **Setup** page; re-paste from the Cloud Resources tab if your session token has expired). This exercise also requires `BUCKET_NAME` and `GLUE_ROLE_ARN` in `/workspace/.env` — see the IAM role setup below for `GLUE_ROLE_ARN`.

### IAM Role Setup
Create Glue service role with permissions:
```bash
aws iam create-role \
  --role-name GlueServiceRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "glue.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam attach-role-policy \
  --role-name GlueServiceRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
  --role-name GlueServiceRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
```

Add to `/workspace/.env`:
```
GLUE_ROLE_ARN=arn:aws:iam::YOUR_ACCOUNT:role/GlueServiceRole
BUCKET_NAME=your-bucket-name
```

## Instructions

### Step 1: Implement Starter Code

Open `glue_catalog_starter.py` and complete the TODOs:

#### TODO 1: Create Glue Database
```python
def create_glue_database():
    try:
        glue.create_database(
            DatabaseInput={
                'Name': DATABASE_NAME,
                'Description': 'Metadata repository for SwiftShop'
            }
        )
        print(f"✓ Database '{DATABASE_NAME}' created")
    except glue.exceptions.AlreadyExistsException:
        print(f"⚠️  Database already exists")
```

#### TODO 2: Upload Sample Data
```python
def upload_sample_data():
    for i in range(3):
        date = (datetime.now() - timedelta(days=2-i)).strftime('%Y-%m-%d')
        
        # Generate orders
        orders = [{
            'order_id': i * 50 + j,
            'user_id': f"user_{j:05d}",
            'order_value': round(np.random.uniform(10, 500), 2),
            'status': np.random.choice(['pending', 'shipped']),
            'created_at': datetime.now()
        } for j in range(50)]
        
        df = pd.DataFrame(orders)
        
        # Upload to S3 with partition
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        key = f"swiftshop/orders/order_date={date}/data.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())
```

#### TODO 3: Create Crawler
```python
def create_crawler():
    glue.create_crawler(
        Name=CRAWLER_NAME,
        Role=IAM_ROLE,
        DatabaseName=DATABASE_NAME,
        Targets={
            'S3Targets': [
                {'Path': f's3://{BUCKET_NAME}/swiftshop/orders/'}
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',  # Handle schema evolution
            'DeleteBehavior': 'LOG'
        }
    )
```

#### TODO 4: Run Crawler
```python
def run_crawler():
    glue.start_crawler(Name=CRAWLER_NAME)
    
    # Wait for completion
    while True:
        response = glue.get_crawler(Name=CRAWLER_NAME)
        state = response['Crawler']['State']
        
        if state == 'READY':
            last_crawl = response['Crawler'].get('LastCrawl', {})
            if last_crawl.get('Status') == 'SUCCEEDED':
                print("✓ Crawler completed")
                break
        
        time.sleep(5)
```

#### TODO 5: Examine Table
```python
def examine_table():
    response = glue.get_table(DatabaseName=DATABASE_NAME, Name='orders')
    table = response['Table']
    
    # Show schema
    print("\nSchema:")
    for col in table['StorageDescriptor']['Columns']:
        print(f"  {col['Name']:<20} {col['Type']}")
    
    # Show partitions
    partitions = glue.get_partitions(DatabaseName=DATABASE_NAME, TableName='orders')
    print(f"\nPartitions: {len(partitions['Partitions'])}")
```

### Step 2: Run Exercise
```bash
python glue_catalog_starter.py
```

### Step 3: Verify in AWS Console

**Glue Console**:
1. Navigate to AWS Glue → Databases
2. Click `swiftshop_catalog`
3. View `orders` table
4. Check schema and partitions

**Athena Console**:
1. Select database: `swiftshop_catalog`
2. Run query:
```sql
SELECT * FROM orders LIMIT 10;
```

### Step 4: Query with Athena CLI
```bash
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM swiftshop_catalog.orders" \
  --query-execution-context Database=swiftshop_catalog \
  --result-configuration OutputLocation=s3://${BUCKET_NAME}/athena-results/
```

## Expected Results

### Crawler Output
```
[Step 4] Running Crawler...
  Starting crawler...
  ✓ Crawler completed successfully
  Tables created: 1
  Partitions created: 3
```

### Table Schema
```
Schema:
  order_id             bigint
  user_id              string
  product_id           string
  order_value          double
  status               string
  created_at           timestamp

Partition Keys:
  order_date (string)

Partitions: 3
  2026-03-01
  2026-03-02
  2026-03-03
```

## Key Concepts

### Schema Inference
Crawler reads Parquet metadata:
```
Parquet Type → Glue Type
int64        → bigint
float64      → double
string       → string
timestamp    → timestamp
```

### Partition Discovery
Recognizes folder patterns:
```
s3://bucket/orders/order_date=2026-03-01/  → Partition: order_date='2026-03-01'
s3://bucket/orders/order_date=2026-03-02/  → Partition: order_date='2026-03-02'
```

### Schema Evolution
When new columns appear:
1. Crawler detects change
2. Updates table schema
3. Old partitions: NULL for new columns
4. New partitions: Values populated

## Common Issues

| Issue | Solution |
|-------|----------|
| `Access Denied` | Check IAM role has S3 read permissions |
| `No partitions found` | Verify folder uses `key=value` format |
| `Crawler fails` | Check S3 path exists and has data |
| `Schema not updated` | Set UpdateBehavior to UPDATE_IN_DATABASE |

## Verification Commands

```bash
# List databases
aws glue get-databases

# Get table details
aws glue get-table \
  --database-name swiftshop_catalog \
  --name orders

# List partitions
aws glue get-partitions \
  --database-name swiftshop_catalog \
  --table-name orders

# Query with Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM swiftshop_catalog.orders WHERE order_date='2026-03-01'" \
  --query-execution-context Database=swiftshop_catalog \
  --result-configuration OutputLocation=s3://${BUCKET_NAME}/athena-results/
```

## Discussion Questions

1. **Why automatic schema inference?**
   - Eliminates manual DDL
   - Reduces errors
   - Handles schema evolution

2. **When to run crawler?**
   - **Hourly**: Regular batch loads
   - **Daily**: Less frequent updates
   - **On-demand**: Ad-hoc data arrival

3. **Cost considerations?**
   - Crawler: $0.44/DPU-hour
   - Typical run: 1-2 minutes
   - Hourly schedule: ~$15/month

## Next Steps

After completing this exercise:
- Understand Glue Crawler automation
- Know how schema inference works
- Can query cataloged data with Athena
- Ready for Lesson 3 (Bronze to Silver transformations)

---

**Time to Complete**: 15-20 minutes  
**Difficulty**: Intermediate  
**Tools**: Python, boto3, AWS Glue
