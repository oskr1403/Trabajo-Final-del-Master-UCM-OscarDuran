import boto3
import os
import tempfile
import io
from dotenv import load_dotenv
import cdsapi

# Load environment variables (only needed if running locally with a .env file)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Ensure AWS credentials and region are correctly set
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError("AWS credentials or region are not set properly.")

# Define dataset and request
dataset = "sis-agroproductivity-indicators"
request = {
    'product_family': ['crop_productivity_indicators'],
    'variable': ['crop_development_stage'],
    'crop_type': ['maize'],
    'year': '2023',
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['10', '20', '28', '30', '31'],
    'growing_season': ['1st_season_per_campaign'],
    'harvest_year': '2023',
    'data_format': 'zip'
}


client = cdsapi.Client()

# Method 2: Temporary File
try:
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name
        print(f"Attempting to retrieve data to temporary file: {temp_file_path}")

        # Retrieve data and save it to the temporary file
        response = client.retrieve(dataset, request)
        response.download(temp_file_path)
        print(f"Data download completed. File saved to: {temp_file_path}")

        # Check if file is empty
        if os.path.getsize(temp_file_path) > 0:
            # Upload the temporary file to S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )

            s3_key = 'CO2.zip'
            s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_key)
            print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")
        else:
            print("Temporary file is empty. No data was retrieved.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Clean up the temporary file
    try:
        os.remove(temp_file_path)
    except:
        pass
