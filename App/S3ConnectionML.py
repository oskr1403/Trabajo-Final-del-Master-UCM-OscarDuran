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

def process_netcdf_in_chunks(file_path, label, chunk_size=1000, output_dir='/tmp'):
    """Procesar archivos NetCDF en chunks y guardar los resultados sin usar dask."""
    try:
        ds = xr.open_dataset(file_path)
        ds = ds.assign_coords(variable_label=("variable_label", [label]))

        # Procesar los datos en chunks manualmente
        for i in range(0, ds.sizes['time'], chunk_size):
            chunk = ds.isel(time=slice(i, i + chunk_size))
            output_file = os.path.join(output_dir, f"{label}_processed_chunk_{i}.nc")
            chunk.to_netcdf(output_file)
            print(f"Chunk {i} procesado y guardado en {output_file}")

            # Imprimir las coordenadas del chunk procesado
            print(f"\nCoordenadas del Chunk {i}:")
            print(chunk.coords.to_dataset().to_dataframe())
        
    except Exception as e:
        print(f"Error al procesar {file_path} en chunks: {str(e)}")

def process_single_file(s3_key, label, chunk_size=1000, output_dir='/tmp'):
    """Procesar un único archivo ZIP de S3, extraer y guardar los resultados en chunks."""
    extract_path = download_and_extract_zip_from_s3(s3_key, extract_to=output_dir)
    
    if extract_path:
        extracted_files = [f for f in os.listdir(extract_path) if f.endswith('.nc')]
        if extracted_files:
            for file in extracted_files:
                full_path = os.path.join(extract_path, file)
                process_netcdf_in_chunks(full_path, label, chunk_size=chunk_size, output_dir=output_dir)
        else:
            print(f"No se encontraron archivos NetCDF en {extract_path}")
    else:
        print(f"Archivo para '{label}' no encontrado en S3")

def main():
    year = "2023"  # Año a procesar
    chunk_size = 1000  # Tamaño del chunk para procesar

    file_to_label = {
        f'crop_development_stage_year_{year}.zip': 'DVS',
        f'total_above_ground_production_year_{year}.zip': 'TAGP',
        f'total_weight_storage_organs_year_{year}.zip': 'TWSO'
    }

    for s3_file, label in file_to_label.items():
        s3_key = f'crop_productivity_indicators/{year}/{s3_file}'
        process_single_file(s3_key, label, chunk_size=chunk_size)
        
        # Pausar entre cada archivo para evitar sobrecargar el sistema
        time.sleep(10)

if __name__ == "__main__":
    main()
