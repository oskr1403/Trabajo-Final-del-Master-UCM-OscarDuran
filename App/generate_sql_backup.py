import pyodbc
import pandas as pd
import os
import boto3
import tempfile

# Credenciales para AWS S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-2"
BUCKET_NAME = "trabajofinalmasterucmoscarduran"

# Conexión a SQL Server con Windows Authentication
server = r'DESKTOP-1SPKENO\SQLEXPRESS'  # Nombre del servidor
database = 'CropProductivityDB'  # Nombre de la base de datos
driver = '{ODBC Driver 17 for SQL Server}'

# Conexión a la base de datos usando autenticación de Windows
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes')
cursor = conn.cursor()

# Crear tablas e insertar los datos de los CSV
def create_table_and_insert_data(file_path, table_name):
    df = pd.read_csv(file_path)
    
    # Crear una tabla en SQL Server
    create_table_query = f"""
    CREATE TABLE {table_name} (
        time DATETIME,
        lat FLOAT,
        lon FLOAT,
        value FLOAT,
        variable VARCHAR(50)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insertar los datos en la tabla
    for _, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {table_name} (time, lat, lon, value, variable)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, row['time'], row['lat'], row['lon'], row['value'], row['variable'])
    
    conn.commit()

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

# Crear un backup de la base de datos
def backup_database_to_file(backup_file_path):
    backup_query = f"""
    BACKUP DATABASE {database}
    TO DISK = '{backup_file_path}'
    WITH FORMAT;
    """
    cursor.execute(backup_query)
    conn.commit()

# Subir el archivo .bak a S3
def upload_backup_to_s3(backup_file_path, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    s3_client.upload_file(backup_file_path, BUCKET_NAME, s3_key)
    print(f"Backup {backup_file_path} uploaded to S3 at {s3_key}")

# Crear un archivo temporal para el backup
with tempfile.NamedTemporaryFile(delete=False, suffix='.bak') as temp_file:
    backup_file_path = temp_file.name

    # Realizar el backup
    backup_database_to_file(backup_file_path)

    # Subir el archivo .bak a S3
    s3_key = f"backups/{database}.bak"
    upload_backup_to_s3(backup_file_path, s3_key)

# Cerrar la conexión a la base de datos
cursor.close()
conn.close()
