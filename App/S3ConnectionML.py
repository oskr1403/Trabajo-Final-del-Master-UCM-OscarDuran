import os
import boto3
import tempfile
import zipfile
import xarray as xr
import numpy as np
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"  # Actualizado a us-east-2
BUCKET_NAME = "trabajofinalmasterucmoscarduran"  # Actualizado al nuevo bucket name

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

def process_files_for_year(year, variables):
    """Descargar, extraer y procesar archivos NetCDF para un año específico, con etiquetas."""
    all_data = []
    for var, var_label in variables.items():
        s3_key = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
        extract_path = download_and_extract_zip_from_s3(s3_key)
        
        if extract_path:
            # Buscar archivos NetCDF extraídos
            extracted_files = [f for f in os.listdir(extract_path) if f.endswith('.nc')]
            if extracted_files:
                for file in extracted_files:
                    full_path = os.path.join(extract_path, file)
                    ds = read_netcdf_with_xarray(full_path, var_label)
                    if ds is not None:
                        print(f"Datos del archivo {file} ({var_label}):")
                        print(ds)
                        all_data.append(ds)
                    else:
                        print(f"No se pudieron cargar los datos para '{var_label}' en {full_path}")
            else:
                print(f"No se encontraron archivos NetCDF en {extract_path}")
        else:
            print(f"Archivo para '{var_label}' en el año {year} no encontrado en S3")
    
    if all_data:
        # Combinar todos los datasets en uno solo
        combined_ds = xr.concat(all_data, dim="variable_label")
        return combined_ds
    else:
        return None

def main():
    # Ajustar las variables con los nombres correspondientes
    variables = {
        'crop_development_stage': 'DVS',
        'total_above_ground_production': 'TAGP',
        'total_weight_storage_organs': 'TWSO'
    }
    years = ["2023"]

    # Procesar en lotes por año
    for year in years:
        combined_ds = process_files_for_year(year, variables)
        if combined_ds is not None:
            print(f"Datos combinados para el año {year}:")
            print(combined_ds)
        else:
            print(f"No se encontraron datos combinados para el año {year}.")

if __name__ == "__main__":
    main()
