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
    print("Uploading script to S3...")
    s3.upload_file(SCRIPT_PATH, S3_BUCKET, S3_SCRIPT_KEY)
    print(f"✓ Script uploaded to s3://{S3_BUCKET}/{S3_SCRIPT_KEY}")

def create_glue_job():
    """Create or update Glue ETL job"""
    print("Creating/updating Glue job...")
    
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
    
    try:
        glue.create_job(Name=JOB_NAME, **job_config)
        print(f"✓ Glue job created: {JOB_NAME}")
    except glue.exceptions.AlreadyExistsException:
        print(f"  Job exists, updating...")
        glue.update_job(JobName=JOB_NAME, JobUpdate=job_config)
        print(f"✓ Glue job updated: {JOB_NAME}")
    except glue.exceptions.IdempotentParameterMismatchException:
        print(f"  Job exists with different config, updating...")
        glue.update_job(JobName=JOB_NAME, JobUpdate=job_config)
        print(f"✓ Glue job updated: {JOB_NAME}")

def run_glue_job():
    """Start Glue job execution"""
    print("Starting Glue job...")
    response = glue.start_job_run(JobName=JOB_NAME)
    job_run_id = response['JobRunId']
    print(f"✓ Job started: {job_run_id}")
    
    # Wait for completion
    print("Waiting for job to complete...")
    while True:
        response = glue.get_job_run(JobName=JOB_NAME, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break
        
        print(f"  Status: {status}")
        time.sleep(10)
    
    if status == 'SUCCEEDED':
        print(f"✓ Job completed successfully")
    else:
        print(f"✗ Job failed with status: {status}")
        if 'ErrorMessage' in response['JobRun']:
            print(f"  Error: {response['JobRun']['ErrorMessage']}")

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
