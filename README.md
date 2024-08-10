# Trabajo-Final-del-Master-UCM-OscarDuran

Documentación del Proyecto: Indicadores de Productividad de Cultivos y Evapotranspiración desde el Año 2000 hasta la Actualidad Derivados de Observaciones Satelitales

1. Introducción
   
Este proyecto tiene como objetivo el desarrollo de una aplicación para la recopilación, procesamiento y almacenamiento de datos sobre indicadores de productividad de cultivos y evapotranspiración desde el año 2000 hasta la actualidad. Estos indicadores se obtienen a partir de observaciones satelitales proporcionadas por el servicio Copernicus Climate Data Store (CDS). Los datos son procesados y almacenados en un bucket de Amazon S3 para su posterior análisis.

3. Estructura del Proyecto
   
El proyecto se organiza en varios jobs, cada uno diseñado para realizar una tarea específica dentro del pipeline de procesamiento de datos. Esta documentación describe el trabajo realizado hasta la fecha y deja espacio para la inclusión de documentación adicional a medida que se implementen nuevos jobs.

4. Arquitectura Técnica

3.1 Componentes y Tecnologías Empleadas
La arquitectura técnica de este proyecto se compone de varios componentes que interactúan para proporcionar una solución completa para la recopilación, procesamiento y almacenamiento de datos de indicadores de productividad de cultivos y evapotranspiración. A continuación, se detallan los componentes clave, sus inter-relaciones y las tecnologías empleadas:

GitHub Actions:

Descripción: GitHub Actions se utiliza para automatizar la ejecución de scripts cada vez que se realiza un push al repositorio.

Tecnologías: YAML, GitHub Actions.

Inter-relaciones: Interactúa con el repositorio de código para desencadenar la ejecución de scripts que se encargan del procesamiento de datos.

API de Copernicus Climate Data Store (CDS):

Descripción: Proporciona acceso a datos satelitales sobre indicadores de productividad de cultivos y evapotranspiración.
Tecnologías: API REST, Python (cdsapi).
Inter-relaciones: Utilizado por los scripts Python para recuperar datos específicos según los parámetros definidos.
AWS S3 (Amazon Simple Storage Service):

Descripción: Servicio de almacenamiento en la nube utilizado para almacenar los datos recuperados de la API de CDS.
Tecnologías: AWS S3, Python (boto3).
Inter-relaciones: Los datos recuperados de CDS son subidos a un bucket de S3 para su almacenamiento y posterior análisis.
Scripts Python:

Descripción: Los scripts Python son responsables de realizar las solicitudes a la API de CDS, manejar los datos descargados y subirlos a AWS S3.
Tecnologías: Python, boto3, cdsapi, dotenv.
Inter-relaciones: Se ejecutan dentro del entorno de GitHub Actions, interactúan con la API de CDS para obtener datos y luego con AWS S3 para almacenar esos datos.
3.2 Flujo de Datos
Trigger de GitHub Actions: Cuando se realiza un push al repositorio, se activa GitHub Actions, que configura el entorno, instala dependencias y ejecuta los scripts Python.
Recuperación de Datos desde CDS: El script Python realiza una solicitud a la API de CDS utilizando parámetros específicos para recuperar los indicadores de productividad de cultivos.
Procesamiento y Almacenamiento en AWS S3: Los datos recuperados se almacenan temporalmente en el sistema de archivos, se validan y luego se suben al bucket de S3 configurado.

4. Caso de Uso de Negocio
   
4.1 Descripción del Caso de Uso
El proyecto aborda la necesidad de monitorear la productividad de cultivos y la evapotranspiración a lo largo del tiempo mediante datos satelitales. Este tipo de monitoreo es esencial para:

Optimización Agrícola: Ayuda a los agricultores y responsables de la toma de decisiones a optimizar el uso de recursos (agua, fertilizantes, etc.) basándose en datos precisos sobre el estado de los cultivos.
Prevención de Desastres: Permite una respuesta proactiva a las condiciones climáticas adversas que pueden afectar la producción agrícola.
Investigación y Desarrollo: Facilita la investigación en la adaptación de cultivos al cambio climático mediante el análisis de datos históricos y actuales.

4.2 Solución Técnica vs. Alternativas Existentes
Solución Técnica Propuesta: La solución desarrollada se basa en la automatización de la recopilación de datos desde la API de CDS, seguida de su almacenamiento en la nube (AWS S3), lo que permite un acceso continuo y escalable a los datos para su posterior análisis.
Mejoras Introducidas:
Automatización: Reduce la intervención manual, minimizando errores y aumentando la eficiencia.
Escalabilidad: La utilización de AWS S3 permite manejar grandes volúmenes de datos sin preocuparse por las limitaciones de almacenamiento.
Integración con Herramientas de Análisis: Los datos almacenados en S3 pueden ser fácilmente integrados con herramientas de análisis y visualización de datos para obtener insights valiosos.
Alternativas Existentes:
Recopilación Manual de Datos: Implica la descarga manual de datos desde fuentes satelitales, lo cual es propenso a errores y consume tiempo.
Almacenamiento Local: Guardar datos en servidores locales puede ser limitado en términos de escalabilidad y accesibilidad, además de requerir mantenimiento adicional.

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
5.3 Propósito y Uso de las Variables
Estas variables son cruciales para especificar exactamente qué datos deseas recuperar de la API. El uso de parámetros específicos como crop_type, growing_season, y variable te permite afinar la solicitud para que solo se descarguen los datos que son relevantes para el análisis que estás realizando.

