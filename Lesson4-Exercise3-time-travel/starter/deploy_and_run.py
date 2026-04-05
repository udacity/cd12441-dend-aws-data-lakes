"""
Deploy and run Time Travel & Schema Evolution Glue job
"""

from load_env import load_env
import boto3
import time
import os

load_env()

s3 = boto3.client('s3')
glue = boto3.client('glue')
sts = boto3.client('sts')

S3_BUCKET = 'swiftshop-data-lake'
SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'time_travel_starter.py')
S3_SCRIPT_KEY = 'glue-scripts/time_travel_starter.py'
JOB_NAME = 'time-travel-schema-evolution'
GLUE_ROLE = 'GlueIcebergETLRole'

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
    print("=== Deploy and Run Time Travel Job ===\n")
    upload_script()
    create_glue_job()
    run_glue_job()
    print(f"\n=== Complete ===")
