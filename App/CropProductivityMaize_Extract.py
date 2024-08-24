import cdsapi
import boto3
import os
import tempfile
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Inicializar cliente de la API de CDS
client = cdsapi.Client()

# Definir parámetros de solicitud para datos bioclimáticos
bioclimatic_request = {
    'variable': ['annual_mean_temperature', 'aridity', 'surface_sensible_heat_flux',
                 'growing_season', 'potential_evaporation', 'volumetric_soil_water'],
    'derived_variable': ['annual_mean', 'coldest_quarter', 'driest_quarter',
                         'start_of_season', 'warmest_quarter', 'wettest_quarter'],
    'model': ['access1_0'],
    'ensemble_member': ['r1i1p1'],
    'experiment': ['rcp4_5'],
    'temporal_aggregation': ['annual'],
    'format': 'zip',
    'version': '1_0',
    'area': 'Europe'  # Ajusta según la región que necesites
}

def upload_to_s3(temp_file_path, s3_client, bucket_name, s3_key):
    """Subir archivo a S3."""
    if os.path.getsize(temp_file_path) > 0:
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)
        print(f"Archivo subido al bucket S3 {bucket_name} con la clave {s3_key}")
    else:
        print("El archivo temporal está vacío. No se recuperaron datos.")

try:
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name
        print(f"Intentando recuperar datos bioclimáticos al archivo temporal: {temp_file_path}")

        # Recuperar los datos
        response = client.retrieve("sis-biodiversity-cmip5-global", bioclimatic_request)
        response.download(temp_file_path)
        print(f"Descarga de datos bioclimáticos completada. Archivo guardado en: {temp_file_path}")

        # Subir el archivo a S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3_key = 'bioclimatic_indicators/bioclimatic_data.zip'
        upload_to_s3(temp_file_path, s3_client, BUCKET_NAME, s3_key)

except Exception as e:
    print(f"Error: {e}")

finally:
    try:
        os.remove(temp_file_path)
    except:
        pass
