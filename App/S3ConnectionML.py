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

# Función para extraer archivos NetCDF de un archivo ZIP y cargar datos específicos por chunks
def extract_and_load_nc_data_by_chunks(zip_file_path, variable_name, chunk_size=1000):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(base_directory)
    
    # Buscar archivos .nc
    nc_files = [os.path.join(base_directory, file) for file in os.listdir(base_directory) if file.endswith('.nc')]
    
    data = []
    
    # Extraer datos de la variable específica por chunks
    for nc_file in nc_files:
        with Dataset(nc_file, 'r') as nc:
            if variable_name in nc.variables:
                var_data = nc.variables[variable_name]
                # Procesar en chunks para evitar sobrecarga de memoria
                for i in range(0, var_data.shape[0], chunk_size):
                    chunk = var_data[i:i+chunk_size].flatten()
                    data.append(chunk)
            else:
                print(f"Advertencia: '{variable_name}' no encontrado en {nc_file}")
    
    if len(data) > 0:
        return np.concatenate(data)
    else:
        return np.array([])  # Retorna un array vacío si no se encuentra la variable

# Verificar si los arrays contienen datos
if len(crop_stage_data) == 0 or len(above_ground_prod_data) == 0 or len(weight_storage_organs_data) == 0:
    raise ValueError("Una o más variables no pudieron ser cargadas. Verifique los nombres de las variables y los archivos NetCDF.")

# Crear un DataFrame para el análisis exploratorio
df = pd.DataFrame({
    "Crop Development Stage (DVS)": crop_stage_data,
    "Total Above Ground Production (TAGP)": above_ground_prod_data,
    "Total Weight Storage Organs (TWSO)": weight_storage_organs_data
})

# Tomar una muestra del 50% de los datos
df_sample = df.sample(frac=0.5, random_state=42)
# Análisis exploratorio con la muestra
print(df_sample.describe())


print("Proceso completado.")
