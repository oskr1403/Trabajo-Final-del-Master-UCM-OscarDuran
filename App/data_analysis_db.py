import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
import tempfile
from dotenv import load_dotenv
from sklearn.cluster import KMeans

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

def download_db_from_s3():
    """Descargar la base de datos SQLite desde S3."""
    s3_key = 'processed_data/crop_productivity.db'
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_file:
        s3_client.download_file(BUCKET_NAME, s3_key, temp_file.name)
        return temp_file.name

def load_table_to_df(db_path, table_name):
    """Cargar una tabla de SQLite a un DataFrame."""
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def perform_clustering(data, n_clusters=3):
    """Realizar clustering KMeans con las variables seleccionadas."""
    features = data[['DVS', 'TAGP', 'TWSO']]
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    data['cluster'] = kmeans.fit_predict(features)
    return data

def visualize_clusters(data):
    """Visualizar los clusters usando un pairplot."""
    sns.pairplot(data, hue='cluster', vars=['DVS', 'TAGP', 'TWSO'])
    plt.savefig('cluster_plot_db.png')

def main():
    # Descargar la base de datos desde S3
    db_path = download_db_from_s3()
    
    # Cargar las tablas de interés de la base de datos a DataFrames
    tables = ['crop_productivity_2019', 'crop_productivity_2020', 'crop_productivity_2021', 'crop_productivity_2022', 'crop_productivity_2023']
    dataframes = [load_table_to_df(db_path, table) for table in tables]
    
    # Combinar todos los DataFrames en uno solo
    combined_data = pd.concat(dataframes)
    
    # Realizar clustering
    clustered_data = perform_clustering(combined_data)
    
    # Visualizar los clusters
    visualize_clusters(clustered_data)
    
    # Guardar el resultado del clustering en un archivo CSV
    clustered_data.to_csv('clustered_crop_data_db.csv', index=False)
    s3_client.upload_file('clustered_crop_data_db.csv', BUCKET_NAME, 'analysis/clustered_crop_data_db.csv')
    print("Análisis completado y resultados subidos a S3.")

if __name__ == "__main__":
    main()
