import os
import boto3
import tempfile
import zipfile
import xarray as xr
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_and_extract_zip_from_s3(s3_key, extract_to='/tmp'):
    """Download and extract ZIP files from S3."""
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
            temp_file_path = temp_file.name
        
        # Extract the ZIP file
        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        
        print(f"File {s3_key} downloaded and extracted to {extract_to}")
        return extract_to
    except Exception as e:
        print(f"Error downloading and extracting {s3_key} from S3: {str(e)}")
        return None

def process_agroclimatic_data(s3_key, output_dir='/tmp'):
    """Process agroclimatic data from S3."""
    extract_path = download_and_extract_zip_from_s3(s3_key, extract_to=output_dir)
    
    if extract_path:
        extracted_files = [f for f in os.listdir(extract_path) if f.endswith('.nc')]
        print(f"Extracted files: {extracted_files}")

        if extracted_files:
            for file in extracted_files:
                full_path = os.path.join(extract_path, file)
                print(f"Processing file: {full_path}")
                ds = xr.open_dataset(full_path)

                # Procesar la variable GSL (Growing season length)
                variable = 'GSL'
                if variable in ds.variables:
                    print(f"Processing variable: {variable}")
                    df = ds[[variable, 'lat', 'lon', 'time']].to_dataframe().reset_index()
                    df = df.dropna(subset=[variable])
                    df.rename(columns={variable: 'value'}, inplace=True)
                    df['variable'] = 'growing_season_length'
                    
                    return df
                else:
                    print(f"Variable {variable} not found in {file}")
        else:
            print(f"No NetCDF files found in {extract_path}")
    else:
        print(f"Data for {s3_key} not found in S3")
    return pd.DataFrame()

def upload_dataframe_to_s3(df, filename):
    """Upload a DataFrame as a CSV file to S3."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
            df.to_csv(temp_csv.name, index=False)
            temp_csv.seek(0)
            s3_key = f'agroclimatic_indicators/processed/{filename}'
            s3_client.upload_file(temp_csv.name, BUCKET_NAME, s3_key)
        print(f"DataFrame uploaded to S3 at {s3_key}")
    except Exception as e:
        print(f"Error uploading DataFrame to S3: {str(e)}")

def main():
    s3_key = 'agroclimatic_indicators/201101_204012_201101_204012.zip'
    df = process_agroclimatic_data(s3_key)
    
    if not df.empty:
        print(f"Processed agroclimatic data:")
        print(df.head())

        # Upload processed DataFrame to S3
        upload_dataframe_to_s3(df, 'agroclimatic_indicators_201101_204012.csv')
    else:
        print(f"No data processed for S3 key {s3_key}")

if __name__ == "__main__":
    main()
