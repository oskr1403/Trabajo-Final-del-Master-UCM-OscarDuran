import boto3
import os
import tempfile
import time
from dotenv import load_dotenv
import cdsapi

if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Credencial de AWS, asegurar que estan correctas
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError("Las credenciales de ASW no son correctas.")

# Define los datasets, el año y las variables.
dataset = "sis-agroproductivity-indicators"
variables = ['crop_development_stage', 'total_above_ground_production', 'total_weight_storage_organs']
years = ['2019', '2020', '2021', '2022', '2023']

client = cdsapi.Client()

def wait_for_job_to_complete(client, request):
    """Espera hasta que el proceso haya terminado."""
    while True:
        try:
            client.retrieve(dataset, request)
            break  # Termina el ciclo si el trabajo esta descargado y listo
        except Exception as e:
            if "Trabajo en proceso" in str(e):
                print("El trabajo se sigue procesando esperar 100 segundos para revisar de nuevo...")
                time.sleep(100)  # Esperar 100 segundo antes de revisar de nuevo
            else:
                raise  # Raise any other exceptions

def upload_to_s3(temp_file_path, s3_client, bucket_name, s3_key):
    """Carga el arhico al S3."""
    if os.path.getsize(temp_file_path) > 0:
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        print(f"File uploaded to S3 bucket {bucket_name} with key {s3_key}")
    else:
        print("El archivo temporal está vacío, no se encontró información.")

try:
    for var in variables:
        for year in years:
            request = {
                'product_family': ['crop_productivity_indicators'],
                'variable': [var],
                'crop_type': ['maize'],
                'year': year,
                'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
                'day': ['10', '20', '28', '30', '31'],
                'growing_season': ['1st_season_per_campaign'],
                'harvest_year': year,
                'data_format': 'zip'
            }

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                print(f"Intentando obtener la información para {var} en el año {year} al archivo temporal: {temp_file_path}")

                # Esperar por el job hasta que descargue
                wait_for_job_to_complete(client, request)

                # Obtener la data y alojarla en un archivo temporal
                response = client.retrieve(dataset, request)
                response.download(temp_file_path)
                print(f"Descarga de datos lista para {var} del año {year}. Archivo guardado en: {temp_file_path}")

                # Subir la información a S3
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION
                )

                s3_key = f'crop_productivity_indicators/{year}/{var}_year_{year}.zip'
                upload_to_s3(temp_file_path, s3_client, BUCKET_NAME, s3_key)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Remover el archivo temporal
    try:
        os.remove(temp_file_path)
    except:
        pass
