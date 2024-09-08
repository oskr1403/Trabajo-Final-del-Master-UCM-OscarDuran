import sqlite3
import pandas as pd
import os
import boto3
import tempfile

# Credenciales para AWS S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Crear la base de datos SQLite
db_file = 'crop_productivity.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Crear tablas e insertar los datos de los CSV
def create_table_and_insert_data(file_path, table_name):
    df = pd.read_csv(file_path)
    
    # Crear una tabla en SQLite
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Datos de {table_name} insertados en la base de datos SQLite.")

# Lista de archivos CSV y nombres de tablas
csv_files = [
    ("/mnt/data/crop_productivity_2019.csv", "CropProductivity2019"),
    ("/mnt/data/crop_productivity_2020.csv", "CropProductivity2020"),
    ("/mnt/data/crop_productivity_2021.csv", "CropProductivity2021"),
    ("/mnt/data/crop_productivity_2022.csv", "CropProductivity2022"),
    ("/mnt/data/crop_productivity_2023.csv", "CropProductivity2023"),
]

# Crear tablas e insertar datos
for file_path, table_name in csv_files:
    create_table_and_insert_data(file_path, table_name)

# Confirmar los cambios
conn.commit()

# Cerrar la conexión
conn.close()

# Subir el archivo .db a S3
def upload_db_to_s3(db_file, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    s3_client.upload_file(db_file, BUCKET_NAME, s3_key)
    print(f"Archivo {db_file} subido a S3 con el nombre {s3_key}")

# Subir el archivo SQLite a S3
s3_key = f"backups/{db_file}"
upload_db_to_s3(db_file, s3_key)
