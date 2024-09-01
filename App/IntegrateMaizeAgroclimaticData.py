import boto3
import pandas as pd
from io import StringIO
import os

# Función para descargar CSV desde S3
def download_csv_from_s3(s3_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='trabajofinalmasterucmoscarduran', Key=s3_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    return df

# Función para subir DataFrame a S3
def upload_dataframe_to_s3(df, s3_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3')
    s3.put_object(Bucket='trabajofinalmasterucmoscarduran', Key=s3_key, Body=csv_buffer.getvalue())
    print(f"DataFrame subido a S3 en {s3_key}")

# Función para combinar los DataFrames con tolerancia
def merge_with_tolerance(df1, df2, tol):
    df1['lat_rounded'] = df1['lat'].round(1)
    df1['lon_rounded'] = df1['lon'].round(1)
    df2['lat_rounded'] = df2['lat'].round(1)
    df2['lon_rounded'] = df2['lon'].round(1)

    merged_df = pd.merge(df1, df2, on=['lat_rounded', 'lon_rounded'], how='inner', suffixes=('_crop', '_agro'))

    return merged_df

def main():
    # Claves correctas para los archivos en S3
    agroclimatic_data_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    
    maize_data_keys = [
        'crop_productivity_indicators/crop_productivity_2019.csv',
        'crop_productivity_indicators/crop_productivity_2020.csv',
        'crop_productivity_indicators/crop_productivity_2021.csv',
        'crop_productivity_indicators/crop_productivity_2022.csv',
        'crop_productivity_indicators/crop_productivity_2023.csv'
    ]
    
    # Descargar el archivo de datos agroclimáticos
    df_agroclimatic = download_csv_from_s3(agroclimatic_data_key)
    
    if not df_agroclimatic.empty:
        df_agroclimatic['time'] = pd.to_datetime(df_agroclimatic['time'], format='%d/%m/%Y', errors='coerce')
        df_agroclimatic['lat'] = df_agroclimatic['lat'].round(2)
        df_agroclimatic['lon'] = df_agroclimatic['lon'].round(2)
        df_agroclimatic.drop_duplicates(subset=['lat', 'lon'], inplace=True)
    
    print(f"Datos agroclimáticos cargados con {len(df_agroclimatic)} registros")

    # Descargar y combinar todos los archivos de datos de maíz
    df_maize_combined = pd.DataFrame()
    for key in maize_data_keys:
        df_maize = download_csv_from_s3(key)
        if not df_maize.empty:
            df_maize['lat'] = df_maize['lat'].round(2)
            df_maize['lon'] = df_maize['lon'].round(2)
            df_maize.drop_duplicates(subset=['lat', 'lon'], inplace=True)

            print(f"Datos de maíz del archivo {key} con {len(df_maize)} registros")
            
            # Realizar la unión (merge) con tolerancia en las coordenadas
            df_combined = merge_with_tolerance(df_maize, df_agroclimatic, tol=0.14) # Tolerancia de ~15 km
            
            if df_combined.empty:
                print(f"Advertencia: La combinación para el archivo {key} resultó en un DataFrame vacío.")
            else:
                print(f"Datos combinados para el archivo {key}: {len(df_combined)} registros")
                df_maize_combined = pd.concat([df_maize_combined, df_combined], ignore_index=True)
    
    if not df_maize_combined.empty:
        output_s3_key = 'Merged data/processed_data/crop_and_agroclimatic_data_combined.csv'
        upload_dataframe_to_s3(df_maize_combined, output_s3_key)
    else:
        print("Error: No se pudieron cargar o combinar los datos de maíz y agroclimáticos.")

if __name__ == "__main__":
    main()
