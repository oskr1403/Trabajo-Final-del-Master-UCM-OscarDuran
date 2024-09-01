import boto3
import os
import pandas as pd
from io import StringIO

# Configura el cliente S3
s3 = boto3.client('s3')
bucket_name = 'trabajofinalmasterucmoscarduran'

def download_csv_from_s3(s3_key):
    """Descargar un archivo CSV desde S3 y cargarlo en un DataFrame."""
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df
    except Exception as e:
        print(f"Error al descargar el archivo CSV desde S3: {e}")
        return None

def upload_dataframe_to_s3(df, filename):
    """Subir un DataFrame como archivo CSV a S3."""
    try:
        with StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=filename, Body=csv_buffer.getvalue())
        print(f"DataFrame subido a S3 en {filename}")
    except Exception as e:
        print(f"Error al subir el DataFrame a S3: {str(e)}")

def filter_agroclimatic_data_by_year(df_agroclimatic, year):
    """Filtrar los datos agroclimáticos para mantener solo el año específico."""
    df_agroclimatic['year_agro'] = pd.to_datetime(df_agroclimatic['time']).dt.year
    return df_agroclimatic[df_agroclimatic['year_agro'] == year]

def main():
    agroclimatic_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    df_agroclimatic = download_csv_from_s3(agroclimatic_key)
    
    if df_agroclimatic is None:
        print("No se pudieron cargar los datos agroclimáticos.")
        return

    print(f"Datos agroclimáticos cargados con {len(df_agroclimatic)} registros")
    
    future_data = []

    for year in range(2019, 2024):
        key = f'processed_data/crop_productivity_{year}.csv'
        df_maize = download_csv_from_s3(key)
        
        if df_maize is None:
            print(f"No se pudieron cargar los datos de maíz para el año {year}.")
            continue

        df_maize['year_crop'] = pd.to_datetime(df_maize['time']).dt.year

        # Filtrar los datos agroclimáticos solo para el año correspondiente
        df_agroclimatic_filtered = filter_agroclimatic_data_by_year(df_agroclimatic, year)
        
        if df_agroclimatic_filtered.empty:
            print(f"Advertencia: No se encontraron datos agroclimáticos para el año {year}.")
            continue

        # Combinar los datos de maíz con los datos agroclimáticos usando lat, lon, y año
        df_combined = pd.merge(df_maize, df_agroclimatic_filtered, how='inner', on=['lat', 'lon'])
        df_combined.drop(columns=['year_crop', 'year_agro'], inplace=True)
        
        output_key = f'Merged_data/processed_data/crop_and_agroclimatic_data_{year}.csv'
        upload_dataframe_to_s3(df_combined, output_key)
        
        print(f"Datos combinados para el año {year} subidos a {output_key}")

    # Crear un archivo para los años futuros
    future_years_key = 'Merged_data/processed_data/crop_and_agroclimatic_data_future.csv'
    future_data = df_agroclimatic[df_agroclimatic['year_agro'] > 2023]
    future_data.drop(columns=['year_agro'], inplace=True)
    upload_dataframe_to_s3(future_data, future_years_key)

if __name__ == "__main__":
    main()
