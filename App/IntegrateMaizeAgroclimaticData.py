import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv
import os

# Cargar variables de entorno
if no os.getenv("GITHUB_ACTIONS"):
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

def merge_with_tolerance(df1, df2, tol=0.5):
    """Realizar un merge aproximado basado solo en la latitud y longitud con una tolerancia de 0.5."""
    
    # Ordenar por latitud y longitud
    df1 = df1.sort_values(by=['lat', 'lon'])
    df2 = df2.sort_values(by=['lat', 'lon'])

    # Realizar el merge_asof para aproximación
    merged_df = pd.merge_asof(df1, df2, on=['lat', 'lon'], tolerance=tol, direction='nearest')

    return merged_df

def main():
    # Claves correctas para los archivos en S3
    agroclimatic_data_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    
    maize_data_keys = [
        'processed_data/crop_productivity_2019.csv',
        'processed_data/crop_productivity_2020.csv',
        'processed_data/crop_productivity_2021.csv',
        'processed_data/crop_productivity_2022.csv',
        'processed_data/crop_productivity_2023.csv'
    ]
    
    # Descargar el archivo de datos agroclimáticos
    df_agroclimatic = download_csv_from_s3(agroclimatic_data_key)
    
    # Convertir la columna 'time' a datetime y extraer 'year'
    if not df_agroclimatic.empty:
        df_agroclimatic['time'] = pd.to_datetime(df_agroclimatic['time'], format='%d/%m/%Y', errors='coerce')
        df_agroclimatic['lat'] = df_agroclimatic['lat'].round(2)
        df_agroclimatic['lon'] = df_agroclimatic['lon'].round(2)
        df_agroclimatic.drop_duplicates(subset=['lat', 'lon'], inplace=True)

    # Descargar y combinar todos los archivos de datos de maíz
    df_maize_combined = pd.DataFrame()
    for key in maize_data_keys:
        df_maize = download_csv_from_s3(key)
        if not df_maize.empty:
            # Redondear 'lat' y 'lon'
            df_maize['lat'] = df_maize['lat'].round(2)
            df_maize['lon'] = df_maize['lon'].round(2)
            df_maize.drop_duplicates(subset=['lat', 'lon'], inplace=True)
            
            # Depuración: Imprimir las primeras filas de los DataFrames
            print(f"Datos de maíz del archivo {key}:")
            print(df_maize.head())
            print("Datos agroclimáticos:")
            print(df_agroclimatic.head())
            
            # Realizar la unión (merge) con tolerancia en las coordenadas
            df_combined = merge_with_tolerance(df_maize, df_agroclimatic, tol=0.5)
            
            # Verificar si la combinación resultó en un DataFrame vacío
            if df_combined.empty:
                print(f"Advertencia: La combinación para el archivo {key} resultó en un DataFrame vacío.")
            else:
                # Agregar el DataFrame combinado al conjunto total
                df_maize_combined = pd.concat([df_maize_combined, df_combined], ignore_index=True)
    
    # Verificar si ambos DataFrames fueron cargados correctamente
    if not df_maize_combined.empty:
        # Subir el DataFrame combinado a S3
        output_s3_key = 'processed_data/crop_and_agroclimatic_data_combined.csv'
        upload_dataframe_to_s3(df_maize_combined, output_s3_key)
    else:
        print("Error: No se pudieron cargar o combinar los datos de maíz y agroclimáticos.")

if __name__ == "__main__":
    main()
