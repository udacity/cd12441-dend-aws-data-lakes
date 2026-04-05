"""
Deploy and run Bronze to Silver ETL Glue job
"""

from load_env import load_env
import boto3
import time
import os

load_env()

# Initialize clients
s3 = boto3.client('s3')
glue = boto3.client('glue')
sts = boto3.client('sts')

# Configuration
S3_BUCKET = 'swiftshop-data-lake'  # Update with your bucket
SCRIPT_PATH = 'bronze_to_silver_etl.py'
S3_SCRIPT_KEY = 'glue-scripts/bronze_to_silver_etl.py'
JOB_NAME = 'bronze-to-silver-etl'
GLUE_ROLE = 'GlueIcebergETLRole'  # Role created by CloudFormation

def upload_script():
    """Upload PySpark script to S3"""
    # TODO: Upload SCRIPT_PATH to S3 using s3.upload_file()
    pass

def create_glue_job():
    """Create or update Glue ETL job"""
    account_id = sts.get_caller_identity()['Account']
    role_arn = f"arn:aws:iam::{account_id}:role/{GLUE_ROLE}"

    job_config = {
        'Role': role_arn,
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': f's3://{S3_BUCKET}/{S3_SCRIPT_KEY}',
            'PythonVersion': '3'
        },
        'GlueVersion': '5.0',
        'WorkerType': 'G.1X',
        'NumberOfWorkers': 2,
        'DefaultArguments': {
            '--enable-glue-datacatalog': 'true',
            '--enable-spark-ui': 'true',
            '--spark-event-logs-path': f's3://{S3_BUCKET}/spark-logs/',
            '--datalake-formats': 'iceberg'
        }
    }

    # TODO: Create the Glue job using glue.create_job()
    # Handle AlreadyExistsException by calling glue.update_job()
    pass

def run_glue_job():
    """Start Glue job execution and wait for completion"""
    # TODO: Start the job using glue.start_job_run()
    # TODO: Poll glue.get_job_run() until status is terminal
    # TODO: Print success or failure with error message
    pass

if __name__ == "__main__":
    print("=== Deploy and Run Glue ETL Job ===\n")
    print(f"⚠️  Prerequisites:")
    print(f"   1. Deploy CloudFormation: aws cloudformation deploy --template-file glue-role.yaml --stack-name glue-etl-role --capabilities CAPABILITY_NAMED_IAM")
    print(f"   2. Update S3_BUCKET = '{S3_BUCKET}'\n")

    upload_script()
    create_glue_job()
    run_glue_job()

    print(f"\n=== Complete ===")
    print(f"Verify in Athena:")
    print(f"SELECT COUNT(*) FROM swiftshop.silver_orders;")
