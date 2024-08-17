import os
import boto3
import zipfile
import tempfile
import numpy as np
import pandas as pd
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Descargar y descomprimir archivos desde S3
def download_and_extract_zip_from_s3(s3_key, extract_to='/tmp'):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name
        s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
    
    with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    extracted_files = [os.path.join(extract_to, file) for file in os.listdir(extract_to) if file.endswith('.nc')]
    return extracted_files

# Leer y procesar archivos NetCDF
def read_netcdf(file_path):
    with Dataset(file_path, 'r') as nc_file:
        data = {}  # Diccionario para almacenar las variables de interés
        for var_name in nc_file.variables:
            data[var_name] = nc_file.variables[var_name][:]
    return pd.DataFrame(data)

# Subir datos a S3
def upload_to_s3(df, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        df.to_csv(temp_file.name, index=False)
        s3_client.upload_file(temp_file.name, BUCKET_NAME, s3_key)
        print(f"Archivo subido a S3: {s3_key}")

# Variables y años a procesar
variables = ['crop_development_stage', 'total_above_ground_production', 'total_weight_storage_organs']
years = ['2019', '2020', '2021', '2022', '2023']

for var in variables:
    all_data = pd.DataFrame()  # DataFrame para almacenar todos los datos combinados
    for year in years:
        s3_key = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        extracted_files = download_and_extract_zip_from_s3(s3_key)
        
        for file_path in extracted_files:
            df = read_netcdf(file_path)
            all_data = pd.concat([all_data, df], axis=0)
    
    # Dividir los datos en 70% entrenamiento y 30% prueba
    train_data, test_data = train_test_split(all_data, test_size=0.3, random_state=42)
    
    # Subir datos de entrenamiento y prueba a S3
    upload_to_s3(train_data, f'train/{var}_train.csv')
    upload_to_s3(test_data, f'test/{var}_test.csv')

