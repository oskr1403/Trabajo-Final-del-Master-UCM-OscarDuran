import os
import boto3
import zipfile
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR  # Cambiado a SVR
from sklearn.metrics import mean_squared_error  # Cambiado a mean_squared_error
from dotenv import load_dotenv

# Cargar variables de entorno (solo necesario si se ejecuta localmente con un archivo .env)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "geltonas.tech"
ZIP_FILE_KEY = "CO2.zip"

# Descargar y descomprimir archivo desde S3
def download_and_extract_zip(file_key, extract_to='/tmp'):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    # Descargar el archivo ZIP
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    zip_file_path = os.path.join(extract_to, 'temp_file.zip')
    
    with open(zip_file_path, 'wb') as zip_file:
        zip_file.write(response['Body'].read())
    
    # Descomprimir el archivo ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Listar archivos descomprimidos
    extracted_files = os.listdir(extract_to)
    return [os.path.join(extract_to, file) for file in extracted_files if file.endswith('.nc')]

# Imprimir información sobre las variables del archivo NetCDF
def print_netcdf_info(file_path):
    with Dataset(file_path, 'r') as nc_file:
        print("Variables disponibles en el archivo NetCDF:")
        for var_name in nc_file.variables:
            print(f"Variable: {var_name}")
