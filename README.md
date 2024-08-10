# Trabajo-Final-del-Master-UCM-OscarDuran

Documentación del Proyecto: Indicadores de Productividad de Cultivos y Evapotranspiración desde el Año 2000 hasta la Actualidad Derivados de Observaciones Satelitales

1. Introducción
Este proyecto tiene como objetivo el desarrollo de una aplicación para la recopilación, procesamiento y almacenamiento de datos sobre indicadores de productividad de cultivos y evapotranspiración desde el año 2000 hasta la actualidad. Estos indicadores se obtienen a partir de observaciones satelitales proporcionadas por el servicio Copernicus Climate Data Store (CDS). Los datos son procesados y almacenados en un bucket de Amazon S3 para su posterior análisis.

2. Estructura del Proyecto
   
El proyecto se organiza en varios jobs, cada uno diseñado para realizar una tarea específica dentro del pipeline de procesamiento de datos. Esta documentación describe el trabajo realizado hasta la fecha y deja espacio para la inclusión de documentación adicional a medida que se implementen nuevos jobs.

3. Trigger de GitHub Actions
   
3.1 Descripción del Trigger
El trigger de GitHub Actions se activa en cada push al repositorio. Este trigger automatiza el proceso de construcción, configuración del entorno, instalación de dependencias y ejecución del script principal del proyecto.

3.2 Ubicación
El archivo de configuración del trigger se encuentra en la raíz del repositorio bajo el nombre Python application en el directorio .github/workflows.

4. Job: Crop_productivity_indicators_Job1.py
   
4.1 Propósito
Este job tiene como objetivo descargar datos sobre indicadores de productividad de cultivos del servicio CDS y almacenarlos en un bucket de Amazon S3.

4.2 Ubicación del Código
El código de este job se encuentra en el archivo App/Crop_productivity_indicators_Job1.py.

4.3 Descripción del Proceso
Carga de Variables de Entorno: El job primero carga las variables de entorno, especialmente cuando no se está ejecutando en GitHub Actions.
Validación de Credenciales de AWS: Se verifica que las credenciales de AWS y la región estén correctamente configuradas.
Definición del Conjunto de Datos y Solicitud: Se define el conjunto de datos a recuperar de CDS, junto con los parámetros de solicitud, como el tipo de cultivo, año, y formato de datos.
Descarga de Datos: Utiliza la API de CDS para descargar los datos y los guarda en un archivo temporal.
Validación y Subida a S3: Se verifica que el archivo no esté vacío y luego se sube al bucket de S3 especificado.
Manejo de Errores y Limpieza: Se maneja cualquier excepción que ocurra durante el proceso y se limpia el archivo temporal al final.

