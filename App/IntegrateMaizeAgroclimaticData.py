import os
import boto3
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"  # Región actualizada
BUCKET_NAME = "trabajofinalmasterucmoscarduran"  # Bucket actualizado

# Crear cliente S3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

def download_csv_from_s3(s3_key):
    """Descargar un archivo CSV desde S3 y cargarlo en un DataFrame de Pandas."""
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    except Exception as e:
        print(f"Error al descargar el archivo CSV desde S3: {str(e)}")
        return None

def merge_with_tolerance(df1, df2, tol=0.15):
    """Combinar dos DataFrames basados en latitud y longitud con una tolerancia especificada."""
    # Redondear lat y lon para la tolerancia
    df1['lat_rounded'] = df1['lat'].round(1)
    df1['lon_rounded'] = df1['lon'].round(1)
    df2['lat_rounded'] = df2['lat'].round(1)
    df2['lon_rounded'] = df2['lon'].round(1)
    
    # Realizar el merge
    merged_df = pd.merge(df1, df2, on=['lat_rounded', 'lon_rounded'], how='inner', suffixes=('_crop', '_agro'))
    return merged_df

def upload_to_s3(df, filename):
    """Subir un DataFrame como archivo CSV a S3."""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_key = f'Merged_data/processed_data/{filename}'
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"DataFrame subido a S3 en {s3_key}")
    except Exception as e:
        print(f"Error al subir el DataFrame a S3: {str(e)}")

def main():
    agroclimatic_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    df_agroclimatic = download_csv_from_s3(agroclimatic_key)
    
    if df_agroclimatic is None:
        print("No se pudieron cargar los datos agroclimáticos.")
        return

    print(f"Datos agroclimáticos cargados con {len(df_agroclimatic)} registros")

    combined_dfs = []
    for year in range(2019, 2024):
        crop_key = f'crop_productivity_indicators/processed_data/crop_productivity_{year}.csv'
        df_maize = download_csv_from_s3(crop_key)
        
        if df_maize is None:
            print(f"No se pudieron cargar los datos de maíz para el año {year}.")
            continue
        
        print(f"Datos de maíz del archivo {crop_key}:")
        print(df_maize.head())
        
        # Filtrar agroclimatic por año
        df_agroclimatic_filtered = df_agroclimatic.copy()  # Trabajar con una copia
        
        if not df_agroclimatic_filtered.empty:
            print(f"Datos agroclimáticos filtrados para el año {year}:")
            print(df_agroclimatic_filtered.head())
            df_combined = merge_with_tolerance(df_maize, df_agroclimatic_filtered, tol=0.15)
            combined_dfs.append(df_combined)
        else:
            print(f"Advertencia: No se encontraron datos agroclimáticos para el año {year}.")

    # Concatenar todos los DataFrames combinados
    if combined_dfs:
        final_df = pd.concat(combined_dfs, ignore_index=True)
        upload_to_s3(final_df, 'crop_and_agroclimatic_data_combined.csv')
    else:
        print("Error: No se pudieron combinar los datos de maíz y agroclimáticos.")

if __name__ == "__main__":
    main()
