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


5. Descripción de Variables Utilizadas en el Job
   
5.1 Dataset
dataset: "sis-agroproductivity-indicators"
Descripción: Especifica el conjunto de datos que se va a utilizar. En este caso, corresponde a los indicadores de productividad de cultivos y evapotranspiración derivados de observaciones satelitales desde el año 2000 hasta la actualidad.

5.2 Parámetros de la Solicitud
La solicitud a la API incluye varios parámetros que definen el tipo de datos que se desean recuperar:

product_family: ['crop_productivity_indicators']

Descripción: Define la familia de productos dentro del conjunto de datos. En este caso, se seleccionan los indicadores de productividad de cultivos.
variable: ['crop_development_stage']

Descripción: Especifica la variable específica dentro de la familia de productos. Aquí se selecciona la etapa de desarrollo del cultivo, lo cual es crucial para el seguimiento del crecimiento de los cultivos.
crop_type: ['maize']

Descripción: Define el tipo de cultivo para el cual se desean obtener los indicadores. En este ejemplo, el tipo de cultivo es maíz (maize).
year: '2023'

Descripción: Indica el año para el cual se recuperarán los datos. Aquí se selecciona el año 2023.
month: ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

Descripción: Lista los meses del año seleccionados para la recuperación de datos. En este caso, se incluyen todos los meses del año 2023.
day: ['10', '20', '28', '30', '31']

Descripción: Define los días específicos de cada mes para los cuales se desea obtener datos. Aquí se seleccionan los días 10, 20, 28, 30 y 31 de cada mes.
growing_season: ['1st_season_per_campaign']

Descripción: Especifica la temporada de crecimiento. En este caso, se selecciona la primera temporada de la campaña de cultivo.
harvest_year: '2023'

Descripción: Define el año de cosecha para los datos seleccionados. Aquí se selecciona el año 2023.
data_format: 'zip'

Descripción: Indica el formato en el cual se desean recibir los datos descargados. En este caso, los datos se recuperan en formato ZIP.

