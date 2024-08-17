import os
import boto3
import zipfile
import tempfile
import pandas as pd
import xarray as xr
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

# Leer archivos NetCDF con xarray y convertir a pandas DataFrame
def read_netcdf_with_xarray(file_path):
    print(f"Leyendo archivo NetCDF: {file_path}")
    ds = xr.open_dataset(file_path)
    df = ds.to_dataframe().reset_index()
    ds.close()  # Cerrar el dataset después de su uso para liberar memoria
    return df

# Subir datos a S3
def upload_to_s3(df, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        print(f"Guardando archivo temporal: {temp_file.name}")
        df.to_csv(temp_file.name, index=False)
        
        print(f"Subiendo archivo a S3: {s3_key}")
        try:
            s3_client.upload_file(temp_file.name, BUCKET_NAME, s3_key)
            print(f"Archivo subido a S3: {s3_key}")
        except Exception as e:
            print(f"Error subiendo archivo a S3: {e}")

# Variables y años a procesar
variables = ['crop_development_stage', 'total_above_ground_production', 'total_weight_storage_organs']
years_2019_2022 = ['2019', '2020', '2021', '2022']
year_2023 = ['2023']

def process_years(years, batch_size=1000):
    all_data = pd.DataFrame()  # DataFrame para almacenar todos los datos combinados

    for year in years:
        for var in variables:
            s3_key = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
            extracted_files = download_and_extract_zip_from_s3(s3_key)
            
            for file_path in extracted_files:
                df = read_netcdf_with_xarray(file_path)
                
                # Filtrar las columnas de interés
                if 'Total Above Ground Production (TAGP)' in df.columns and 'Total Weight Storage Organs (TWSO)' in df.columns:
                    X_filtered = df[["Total Above Ground Production (TAGP)", "Total Weight Storage Organs (TWSO)"]]
                else:
                    X_filtered = pd.DataFrame()  # Crear un DataFrame vacío si no existen esas columnas

                if 'Crop Development Stage (DVS)' in df.columns:
                    y_filtered = df["Crop Development Stage (DVS)"]
                else:
                    y_filtered = pd.Series()  # Crear una Serie vacía si no existe la columna

                if not X_filtered.empty and not y_filtered.empty:
                    # Concatenar las columnas X e y en un solo DataFrame
                    df_filtered = pd.concat([X_filtered, y_filtered], axis=1)
                    all_data = pd.concat([all_data, df_filtered], axis=0)
                    
                    # Procesar en batches
                    if len(all_data) >= batch_size:
                        print(f"Procesando batch para el año {year}...")
                        save_batches(all_data, year)
                        all_data = pd.DataFrame()  # Resetear el DataFrame después de guardar el batch

    # Guardar el último batch si queda algo
    if not all_data.empty:
        print(f"Guardando último batch para el año {year}...")
        save_batches(all_data, year)

def save_batches(df, year):
    train_data, test_data = train_test_split(df, test_size=0.2, random_state=42)
    
    # Guardar datos de entrenamiento y prueba en archivos CSV por año
    temp_s3_key_train = f'train/{year}_train.csv'
    temp_s3_key_test = f'test/{year}_test.csv'
    
    print(f"Guardando datos de entrenamiento y prueba para el año {year}...")
    upload_to_s3(train_data, temp_s3_key_train)
    upload_to_s3(test_data, temp_s3_key_test)

# Procesar los años 2019-2022 en conjunto
process_years(years_2019_2022)

# Procesar el año 2023 por separado
process_years(year_2023)

print("Proceso completado.")


