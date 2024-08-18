import os
import boto3
import tempfile
import zipfile
import xarray as xr
import numpy as np
import time
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"  # Región actualizada
BUCKET_NAME = "trabajofinalmasterucmoscarduran"  # Bucket actualizado

# Crear cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_and_extract_zip_from_s3(s3_key, extract_to='/tmp'):
    """Descargar y extraer archivos ZIP desde S3."""
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
            temp_file_path = temp_file.name
        
        # Extraer el archivo ZIP
        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        
        print(f"Archivo {s3_key} descargado y extraído en {extract_to}")
        return extract_to
    except Exception as e:
        print(f"Error al descargar y extraer {s3_key} desde S3: {str(e)}")
        return None

def read_netcdf_with_xarray(file_path, variable_label):
    """Leer archivos NetCDF con xarray y agregar una etiqueta de variable."""
    try:
        ds = xr.open_dataset(file_path)
        # Agregar una nueva coordenada o dimensión que identifique la variable
        ds = ds.assign_coords(variable_label=("variable_label", [variable_label]))
        return ds
    except FileNotFoundError:
        print(f"Archivo {file_path} no encontrado.")
        return None

def process_files_for_year(year):
    """Descargar, extraer y procesar archivos NetCDF para un año específico, con etiquetas."""
    all_data = []
    # Definir el mapeo entre el nombre del archivo y la etiqueta de la variable
    file_to_label = {
        f'crop_development_stage_year_{year}.zip': 'DVS',
        f'total_above_ground_production_year_{year}.zip': 'TAGP',
        f'total_weight_storage_organs_year_{year}.zip': 'TWSO'
    }

    for s3_file, label in file_to_label.items():
        s3_key = f'crop_productivity_indicators/{year}/{s3_file}'
        extract_path = download_and_extract_zip_from_s3(s3_key)
        
        if extract_path:
            # Buscar archivos NetCDF extraídos
            extracted_files = [f for f in os.listdir(extract_path) if f.endswith('.nc')]
            if extracted_files:
                for file in extracted_files:
                    full_path = os.path.join(extract_path, file)
                    ds = read_netcdf_with_xarray(full_path, label)
                    if ds is not None:
                        print(f"Datos del archivo {file} ({label}):")
                        print(ds)
                        all_data.append(ds)
                    else:
                        print(f"No se pudieron cargar los datos para '{label}' en {full_path}")
            else:
                print(f"No se encontraron archivos NetCDF en {extract_path}")
        else:
            print(f"Archivo para '{label}' en el año {year} no encontrado en S3")
        
        # Pausar entre cada archivo para evitar sobrecargar el sistema
        time.sleep(10)  # Pausa de 10 segundos entre cada archivo
    
    if all_data:
        # Combinar todos los datasets en uno solo
        combined_ds = xr.concat(all_data, dim="variable_label")
        return combined_ds
    else:
        return None

def main():
    years = ["2023"]  # Lista de años a procesar

    for year in years:
        # Procesar en lotes por año
        combined_ds = process_files_for_year(year)
        if combined_ds is not None:
            print(f"Datos combinados para el año {year}:")
            print(combined_ds)
            
            # Imprimir las coordenadas para verificar las etiquetas
            print("\nCoordenadas del Dataset Combinado:")
            print(combined_ds.coords)
        else:
            print(f"No se encontraron datos combinados para el año {year}.")
        
        # Pausar entre el procesamiento de años para evitar sobrecarga
        time.sleep(60)  # Pausa de 60 segundos entre el procesamiento de años

if __name__ == "__main__":
    main()
