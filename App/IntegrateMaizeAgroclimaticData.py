def main():
    # Claves correctas para los archivos en S3
    agroclimatic_data_key = 'agroclimatic_indicators/processed/agroclimatic_indicators_2019_2030.csv'
    
    maize_data_keys = [
        'processed_data/crop_productivity_2019.csv',
        'processed_data/crop_productivity_2020.csv',
        'processed_data/crop_productivity_2021.csv',
        'processed_data/crop_productivity_2022.csv',
        'processed_data/crop_productivity_2023.csv'
    ]
    
    # Descargar el archivo de datos agroclimáticos
    df_agroclimatic = download_csv_from_s3(agroclimatic_data_key)
    
    # Convertir la columna 'time' a datetime y extraer 'year'
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
            df_combined = merge_with_tolerance(df_maize, df_agroclimatic, tol=0.1)
            
            # Verificar si la combinación resultó en un DataFrame vacío
            if df_combined.empty:
                print(f"Advertencia: La combinación para el archivo {key} resultó en un DataFrame vacío.")
            else:
                print(f"Datos combinados para el archivo {key}: {len(df_combined)} registros")
                df_maize_combined = pd.concat([df_maize_combined, df_combined], ignore_index=True)
    
    if not df_maize_combined.empty:
        output_s3_key = 'processed_data/crop_and_agroclimatic_data_combined.csv'
        upload_dataframe_to_s3(df_maize_combined, output_s3_key)
    else:
        print("Error: No se pudieron cargar o combinar los datos de maíz y agroclimáticos.")

if __name__ == "__main__":
    main()
