import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import NoCredentialsError

def download_csv_from_s3(s3_key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket='trabajofinalmasterucmoscarduran', Key=s3_key)
        data = obj['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(data))
    except NoCredentialsError:
        print("Credenciales no disponibles")
        return None
    except s3.exceptions.NoSuchKey:
        print(f"Error al descargar el archivo CSV desde S3: No se pudo encontrar la clave {s3_key}.")
        return None

def merge_with_tolerance(df1, df2, tol=0.1):
    df1['lat_rounded'] = df1['lat'].round(1)
    df1['lon_rounded'] = df1['lon'].round(1)
    df2['lat_rounded'] = df2['lat'].round(1)
    df2['lon_rounded'] = df2['lon'].round(1)

    merged_df = pd.merge(df1, df2, on=['lat_rounded', 'lon_rounded'], how='inner', suffixes=('_crop', '_agro'))
    return merged_df

def main():
    agroclimatic_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    df_agroclimatic = download_csv_from_s3(agroclimatic_key)
    if df_agroclimatic is not None:
        print(f"Datos agroclimáticos cargados con {len(df_agroclimatic)} registros")
    else:
        print("No se pudieron cargar los datos agroclimáticos.")
        return

    for year in range(2019, 2024):
        crop_key = f'processed_data/crop_productivity_{year}.csv'
        df_maize = download_csv_from_s3(crop_key)
        if df_maize is not None:
            print(f"Datos de maíz del archivo {crop_key}:")
            print(df_maize.head())
            df_combined = merge_with_tolerance(df_maize, df_agroclimatic, tol=0.15)
            if not df_combined.empty:
                print(f"Datos combinados para el año {year}:")
                print(df_combined.head())
            else:
                print(f"No se encontraron coincidencias en los datos combinados para el año {year}.")
        else:
            print(f"No se pudieron cargar los datos de maíz para el año {year}.")
            continue

if __name__ == "__main__":
    main()
