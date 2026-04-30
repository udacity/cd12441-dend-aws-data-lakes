"""
Grant Lake Formation permissions for Glue ETL role to access S3 Tables
"""

import boto3
from load_env import load_env

load_env()

session = boto3.Session(region_name='us-east-1')
lf = session.client('lakeformation')
sts = session.client('sts')

ACCOUNT_ID = sts.get_caller_identity()['Account']
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/ecommerce-analytics-glue-role-dev"
TABLE_BUCKET = 'swiftshop-analytics-tables'
CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET}"

# TODO: Define grants list with three entries:
#   1. Catalog-level: resource={'Catalog': {'Id': CATALOG_ID}}, permissions=['ALL']
#   2. Database-level: resource={'Database': {'CatalogId': CATALOG_ID, 'Name': 'swiftshop'}}, permissions=['ALL']
#   3. Table-level: resource={'Table': {'CatalogId': CATALOG_ID, 'DatabaseName': 'swiftshop', 'TableWildcard': {}}}, permissions=['ALL']
grants = # YOUR CODE HERE

print(f"Granting Lake Formation permissions to {ROLE_ARN}\n")

for g in grants:
    try:
        # TODO: Call lf.grant_permissions() with Principal, Resource, and Permissions
        # YOUR CODE HERE
        print(f"✓ Granted {g['permissions']} on {g['desc']}")
    except Exception as e:
        print(f"✗ {g['desc']}: {e}")

print("\nDone.")
