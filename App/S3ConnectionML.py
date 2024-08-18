import os
import boto3
import tempfile
import xarray as xr
import numpy as np
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Crear cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_netcdf_from_s3(s3_key, extract_to='/tmp'):
    """Descargar archivo NetCDF desde S3."""
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            s3_client.download_fileobj(BUCKET_NAME, s3_key, temp_file)
            temp_file_path = temp_file.name
        print(f"Archivo {s3_key} descargado en {temp_file_path}")
        return temp_file_path
    except Exception as e:
        print(f"Error al descargar {s3_key} desde S3: {str(e)}")
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
    """Descargar y procesar archivos NetCDF para un año específico, con etiquetas."""
    all_data = []
    for var, var_label in variables.items():
        s3_key = f'crop_productivity_indicators/{year}/{var}_year_{year}.nc'
        file_path = download_netcdf_from_s3(s3_key)
        
        if file_path:
            ds = read_netcdf_with_xarray(file_path, var_label)
            if ds is not None:
                print(f"Datos del archivo {s3_key} ({var_label}):")
                print(ds)
                all_data.append(ds)
            else:
                print(f"No se pudieron cargar los datos para '{var_label}' en {file_path}")
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
        'DVS': 'crop_development_stage',
        'TAGP': 'total_above_ground_production',
        'TWSO': 'total_weight_storage_organs'
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
