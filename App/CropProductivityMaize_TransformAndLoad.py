import os
import boto3
import tempfile
import zipfile
import xarray as xr
import pandas as pd
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

def process_single_file(s3_key, variable_name, output_dir='/tmp', expected_year=None):
    """Procesar un único archivo ZIP de S3, extraer y filtrar los valores no nulos de una variable."""
    extract_path = download_and_extract_zip_from_s3(s3_key, extract_to=output_dir)
    
    if extract_path:
        extracted_files = [f for f in os.listdir(extract_path) if f.endswith('.nc')]
        if extracted_files:
            for file in extracted_files:
                full_path = os.path.join(extract_path, file)
                ds = xr.open_dataset(full_path)

                # Mostrar las variables disponibles en el archivo
                print(f"Variables disponibles en el archivo {file}: {list(ds.variables.keys())}")

                # Verificar si la variable existe en el dataset
                if variable_name not in ds.variables:
                    print(f"La variable '{variable_name}' no se encontró en el archivo {file}.")
                    continue

                # Filtrar los valores no nulos de la variable de interés y filtrar por año
                df = ds[[variable_name, 'lat', 'lon', 'time']].to_dataframe().reset_index()
                df = df.dropna(subset=[variable_name])  # Eliminar filas con valores nulos en la variable
                
                # Filtrar por el año esperado en la columna 'time'
                df['year'] = pd.to_datetime(df['time']).dt.year
                df = df[df['year'] == int(expected_year)]  # Filtrar por el año correspondiente

                # Renombrar la columna de la variable
                df.rename(columns={variable_name: 'value'}, inplace=True)
                
                # Añadir una columna para identificar la variable
                df['variable'] = variable_name

                return df
        else:
            print(f"No se encontraron archivos NetCDF en {extract_path}")
    else:
        print(f"Archivo para '{variable_name}' no encontrado en S3")
    return pd.DataFrame()  # Retornar un DataFrame vacío si hay algún error

def upload_dataframe_to_s3(df, filename):
    """Subir un DataFrame como archivo CSV a S3."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
            df.to_csv(temp_csv.name, index=False)
            temp_csv.seek(0)
            s3_key = f'processed_data/{filename}'
            s3_client.upload_file(temp_csv.name, BUCKET_NAME, s3_key)
        print(f"DataFrame subido a S3 en {s3_key}")
    except Exception as e:
        print(f"Error al subir el DataFrame a S3: {str(e)}")

def main():
    years = ["2023", "2022", "2021", "2020", "2019"]  # Años a procesar

    # Diccionario con los archivos y las variables
    file_to_variable_template = {
        'crop_development_stage_year_{year}.zip': 'DVS',
        'total_above_ground_production_year_{year}.zip': 'TAGP',
        'total_weight_storage_organs_year_{year}.zip': 'TWSO'
    }

    for year in years:
        dfs = []

        for file_template, variable_name in file_to_variable_template.items():
            s3_file = file_template.format(year=year)
            s3_key = f'crop_productivity_indicators/{year}/{s3_file}'
            df = process_single_file(s3_key, variable_name, expected_year=year)
            if not df.empty:
                dfs.append(df)

        # Combinar los DataFrames en uno solo
        if dfs:
            combined_df = pd.concat(dfs, axis=0)
            print(f"DataFrame combinado para el año {year}:")
            print(combined_df.head())  # Mostrar solo las primeras filas

            # Subir el DataFrame combinado a S3
            upload_dataframe_to_s3(combined_df, f'crop_productivity_{year}.csv')
        else:
            print(f"No se pudieron procesar archivos o no hay datos no nulos para el año {year}.")

if __name__ == "__main__":
    main()
