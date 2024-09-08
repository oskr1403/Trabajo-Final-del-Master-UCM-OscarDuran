import os
import boto3
import sqlite3
import pandas as pd
from dotenv import load_dotenv
import tempfile

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
    """Descargar un archivo CSV desde S3."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            s3_client.download_file(BUCKET_NAME, s3_key, temp_file.name)
            print(f"Archivo CSV {s3_key} descargado correctamente.")
            return temp_file.name
    except Exception as e:
        print(f"Error al descargar el archivo {s3_key} desde S3: {str(e)}")
        return None

def create_sqlite_db(db_path, csv_files):
    """Crear una base de datos SQLite y cargar los datos de los CSV."""
    conn = sqlite3.connect(db_path)
    try:
        for csv_file, table_name in csv_files.items():
            # Leer el CSV en un DataFrame
            df = pd.read_csv(csv_file)
            # Guardar el DataFrame como una tabla en la base de datos
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Tabla '{table_name}' creada en la base de datos desde el archivo {csv_file}.")
    except Exception as e:
        print(f"Error al crear la base de datos SQLite: {str(e)}")
    finally:
        conn.close()

def upload_db_to_s3(db_path):
    """Subir el archivo .db a S3."""
    try:
        s3_key = 'processed_data/crop_productivity.db'
        s3_client.upload_file(db_path, BUCKET_NAME, s3_key)
        print(f"Base de datos SQLite subida correctamente a S3 en {s3_key}.")
    except Exception as e:
        print(f"Error al subir la base de datos a S3: {str(e)}")

def main():
    # Lista de archivos CSV en S3 y nombres de tablas correspondientes
    csv_files_s3 = {
        'processed_data/crop_productivity_2019.csv': 'crop_productivity_2019',
        'processed_data/crop_productivity_2020.csv': 'crop_productivity_2020',
        'processed_data/crop_productivity_2021.csv': 'crop_productivity_2021',
        'processed_data/crop_productivity_2022.csv': 'crop_productivity_2022',
        'processed_data/crop_productivity_2023.csv': 'crop_productivity_2023'
    }
    
    # Descargar los archivos CSV de S3
    csv_files_local = {}
    for s3_key, table_name in csv_files_s3.items():
        csv_file_local = download_csv_from_s3(s3_key)
        if csv_file_local:
            csv_files_local[csv_file_local] = table_name
    
    # Crear la base de datos SQLite y cargar los CSV en ella
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_db:
        db_path = temp_db.name
        create_sqlite_db(db_path, csv_files_local)

        # Subir la base de datos SQLite a S3
        upload_db_to_s3(db_path)

if __name__ == "__main__":
    main()
