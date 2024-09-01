import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv
import os

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

def download_csv_from_s3(s3_key):
    """Descargar un archivo CSV desde S3 y cargarlo en un DataFrame de pandas."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        print(f"Archivo CSV {s3_key} descargado y cargado en DataFrame.")
        return df
    except Exception as e:
        print(f"Error al descargar el archivo CSV desde S3: {str(e)}")
        return pd.DataFrame()

def upload_dataframe_to_s3(df, s3_key):
    """Subir un DataFrame como archivo CSV a S3."""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"DataFrame subido a S3 en {s3_key}")
    except Exception as e:
        print(f"Error al subir el DataFrame a S3: {str(e)}")

def main():
    # Descargar los archivos CSV desde S3
    maize_data_key = 'ruta/a/tu/datos_maiz.csv'  # Reemplaza con la clave S3 correcta
    agroclimatic_data_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    
    df_maize = download_csv_from_s3(maize_data_key)
    df_agroclimatic = download_csv_from_s3(agroclimatic_data_key)
    
    # Verificar si ambos DataFrames fueron cargados correctamente
    if not df_maize.empty and not df_agroclimatic.empty:
        # Asegurarse de que la columna 'time' esté en formato datetime en ambos DataFrames
        df_maize['time'] = pd.to_datetime(df_maize['time'])
        df_agroclimatic['time'] = pd.to_datetime(df_agroclimatic['time'])
        
        # Realizar la unión (merge) por 'lat', 'lon', y 'time'
        df_combined = pd.merge(df_maize, df_agroclimatic, on=['lat', 'lon', 'time'], how='inner')
        
        # Subir el DataFrame combinado a S3
        output_s3_key = 'ruta/a/tu/datos_combinados.csv'  # Reemplaza con la clave S3 deseada
        upload_dataframe_to_s3(df_combined, output_s3_key)
    else:
        print("Error: No se pudieron cargar los datos de maíz o agroclimáticos.")

if __name__ == "__main__":
    main()
