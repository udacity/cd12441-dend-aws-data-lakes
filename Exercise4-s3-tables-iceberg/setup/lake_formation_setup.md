# Lake Formation Setup for S3 Tables Integration

## Step 1: Enable S3 Tables in Lake Formation

1. **Navigate to Lake Formation Console**
   - Go to AWS Lake Formation console
   - Select your AWS region

2. **Enable S3 Tables**
   - Go to "Settings" → "S3 Tables"
   - Click "Enable S3 Tables"
   - Create or select S3 bucket for S3 Tables metadata

3. **Configure External Engine Integration**
   - Go to "Settings" → "App integrations"
   - Enable "Allow external engines to access S3 Tables"
   - Add AWS Glue as authorized service

## Step 2: Create S3 Tables Bucket

```bash
# Create dedicated bucket for S3 Tables metadata
aws s3 mb s3://cloudmart-s3tables-metadata --region us-east-1

# Enable versioning for metadata protection
aws s3api put-bucket-versioning \
  --bucket cloudmart-s3tables-metadata \
  --versioning-configuration Status=Enabled
```

## Step 3: Configure IAM Role for Glue

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lakeformation:GetDataAccess",
        "lakeformation:GrantPermissions",
        "lakeformation:RevokePermissions",
        "lakeformation:BatchGrantPermissions",
        "lakeformation:BatchRevokePermissions",
        "lakeformation:ListPermissions"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::cloudmart-s3tables-metadata",
        "arn:aws:s3:::cloudmart-s3tables-metadata/*",
        "arn:aws:s3:::cloudmart",
        "arn:aws:s3:::cloudmart/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": "*"
    }
  ]
}
```

## Step 4: Grant Lake Formation Permissions

```bash
# Grant database permissions to Glue role
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/AWSGlueServiceRole \
  --permissions "CREATE_TABLE,ALTER,DROP" \
  --resource Database='{Name=cloudmart_db}'

# Grant table permissions
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/AWSGlueServiceRole \
  --permissions "SELECT,INSERT,DELETE,ALTER" \
  --resource Table='{DatabaseName=cloudmart_db,Name=bronze_orders}'
```

## Step 5: Verify Setup

1. **Check S3 Tables Status**
   - Lake Formation console → Settings → S3 Tables
   - Verify "Enabled" status

2. **Test Permissions**
   - Create test database in Glue console
   - Verify Glue role can access S3 Tables bucket

3. **Validate External Engine Access**
   - Settings → App integrations
   - Confirm AWS Glue is listed as authorized service

## Troubleshooting

### Common Issues:
- **Permission Denied**: Check Lake Formation permissions and IAM policies
- **S3 Access Errors**: Verify S3 bucket policies and cross-account access
- **Iceberg Configuration**: Ensure Glue 5.0 and correct Spark extensions

### Verification Commands:
```bash
# Test S3 Tables bucket access
aws s3 ls s3://cloudmart-s3tables-metadata/

# Check Lake Formation permissions
aws lakeformation list-permissions --principal arn:aws:iam::ACCOUNT:role/AWSGlueServiceRole

# Verify Glue catalog access
aws glue get-databases
```
