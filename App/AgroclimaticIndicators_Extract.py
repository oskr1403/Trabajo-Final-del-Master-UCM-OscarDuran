import boto3
import os
import tempfile
import time
from dotenv import load_dotenv
import cdsapi

# Load environment variables
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Ensure AWS credentials and region are correctly set
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError("AWS credentials or region are not set properly.")

# Define dataset and variables
client = cdsapi.Client()

request = {
    'origin': 'hadgem2_es_model',
    'variable': ['growing_season_length', 'precipitation_sum', 'mean_temperature'],
    'experiment': 'rcp2_6',
    'temporal_aggregation': 'annual',
    'period': ['201101_204012'],
    'version': ['1_1']
}

def wait_for_job_to_complete(client, request):
    """Wait until the job is completed before attempting to download."""
    while True:
        try:
            client.retrieve('sis-agroclimatic-indicators', request)
            break
        except Exception as e:
            if "Result not ready, job is running" in str(e):
                print("Job is still running, waiting for 10 seconds before checking again...")
                time.sleep(100)
            else:
                raise

def upload_to_s3(temp_file_path, s3_client, bucket_name, s3_key):
    """Upload file to S3."""
    if os.path.getsize(temp_file_path) > 0:
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        print(f"File uploaded to S3 bucket {bucket_name} with key {s3_key}")
    else:
        print("Temporary file is empty. No data was retrieved.")

# Extracting data and uploading to S3
try:
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name
        print(f"Attempting to retrieve agroclimatic data to temporary file: {temp_file_path}")

        wait_for_job_to_complete(client, request)

        # Retrieve data and download it directly to the temporary file
        response = client.retrieve('sis-agroclimatic-indicators', request)
        response.download(temp_file_path)
        print(f"Data download completed. File saved to: {temp_file_path}")

        # Upload the file to S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_key = f'agroclimatic_indicators/{request["period"][0]}_{request["period"][-1]}.zip'
        upload_to_s3(temp_file_path, s3_client, BUCKET_NAME, s3_key)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Clean up the temporary file
    try:
        os.remove(temp_file_path)
    except:
        pass
