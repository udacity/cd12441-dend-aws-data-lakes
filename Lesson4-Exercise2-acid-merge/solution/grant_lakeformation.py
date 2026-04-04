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
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/GlueIcebergETLRole"
TABLE_BUCKET = 'swiftshop-analytics-tables'
CATALOG_ID = f"{ACCOUNT_ID}:s3tablescatalog/{TABLE_BUCKET}"

grants = [
    {
        'resource': {'Catalog': {'Id': CATALOG_ID}},
        'permissions': ['ALL'],
        'desc': 'S3 Tables catalog'
    },
    {
        'resource': {'Database': {'CatalogId': CATALOG_ID, 'Name': 'swiftshop'}},
        'permissions': ['ALL'],
        'desc': 'swiftshop database'
    },
    {
        'resource': {'Table': {'CatalogId': CATALOG_ID, 'DatabaseName': 'swiftshop', 'TableWildcard': {}}},
        'permissions': ['ALL'],
        'desc': 'all tables in swiftshop'
    },
]

print(f"Granting Lake Formation permissions to {ROLE_ARN}\n")

for g in grants:
    try:
        lf.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': ROLE_ARN},
            Resource=g['resource'],
            Permissions=g['permissions']
        )
        print(f"✓ Granted {g['permissions']} on {g['desc']}")
    except Exception as e:
        print(f"✗ {g['desc']}: {e}")

print("\nDone.")
