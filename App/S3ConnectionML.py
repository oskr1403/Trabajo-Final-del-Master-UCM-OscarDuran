import os
import boto3
import tempfile
import zipfile
import xarray as xr
import pandas as pd
import sqlite3
from dotenv import load_dotenv

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Conexión a la base de datos SQLite
conn = sqlite3.connect('crop_productivity.db')

def create_table_if_not_exists(conn):
    """Crear la tabla si no existe."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crop_data (
        lat REAL,
        lon REAL,
        time TEXT,
        value REAL,
        variable TEXT,
        year INT
    );
    """
    conn.execute(create_table_query)
    conn.commit()

def insert_data_to_db(conn, df, year):
    """Insertar los datos en la tabla de SQLite."""
    df['year'] = year  # Añadir el año a los datos
    df.to_sql('crop_data', conn, if_exists='append', index=False)

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

def process_single_file(s3_key, variable_name, output_dir='/tmp'):
    """Procesar un único archivo ZIP de S3, extraer y filtrar los valores no nulos de una variable."""
    extract_path = download_and_extract_zip_from_s3(s3_key, extract_to=output_dir)
    
    if extract_path:
        extracted_files = [f for f in os.listdir(extract_path) if f.endswith('.nc')]
        if extracted_files:
            for file in extracted_files:
                full_path = os.path.join(extract_path, file)
                ds = xr.open_dataset(full_path)

                # Verificar si la variable existe en el dataset
                if variable_name not in ds.variables:
                    print(f"La variable '{variable_name}' no se encontró en el archivo {file}.")
                    continue

                # Filtrar los valores no nulos de la variable de interés
                df = ds[[variable_name, 'lat', 'lon', 'time']].to_dataframe().reset_index()
                df = df.dropna(subset=[variable_name])  # Eliminar filas con valores nulos en la variable
                
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

def main():
    create_table_if_not_exists(conn)
    years = ["2022", "2021", "2020", "2019"]
    
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
            df = process_single_file(s3_key, variable_name)
            if not df.empty:
                dfs.append(df)

        if dfs:
            combined_df = pd.concat(dfs, axis=0)
            print(f"DataFrame combinado para el año {year}:")
            print(combined_df.head())  # Mostrar solo las primeras filas
            
            # Insertar los datos en la base de datos
            insert_data_to_db(conn, combined_df, year)

if __name__ == "__main__":
    main()
    conn.close()  # No olvides cerrar la conexión al final
