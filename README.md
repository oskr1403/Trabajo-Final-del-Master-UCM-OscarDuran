Links Importantes

Descarga manual de la información de Copernicus:
https://cds-beta.climate.copernicus.eu/datasets/sis-agroproductivity-indicators?tab=download

Repositorio en GitHub con el código del proyecto:
https://github.com/oskr1403/Trabajo-Final-del-Master-UCM-OscarDuran

Carpeta del pipeline en GitHub:
https://github.com/oskr1403/Trabajo-Final-del-Master-UCM-OscarDuran/tree/main/.github/workflows

Carpeta de los scripts de extracción, transformación y carga, y creación de la base de datos:
https://github.com/oskr1403/Trabajo-Final-del-Master-UCM-OscarDuran/tree/main/App

Descarga de la base de datos consolidada en formato .db desde S3:
https://trabajofinalmasterucmoscarduran.s3.us-east-2.amazonaws.com/processed_data/crop_productivity.db
 

Introducción
Este proyecto de creación de un pipeline de preparación o disponibilización de datos busca desarrollar un sistema que automatice la elaboración y preparación de datos con respecto a la productividad del maíz. La idea principal es integrar indicadores clave y que los datos queden listos para ser analizados de forma eficiente.
Este pipeline logra automatizar el proceso de Extracción, Transformación y Carga (ETL) de datos provenientes de distintas fuentes durante varios años (2019-2023), esto asegura y cumple una integración eficiente y escalable en una base de datos centralizada. Estos datos han sido obtenidos a través de tecnologías de observación satelital, utilizando la API de Copernicus, y son almacenados en Amazon S3 para ser analizados y accesados por el usuario(s) habilitado(s).
Uno de los desafíos más importantes de este proyecto fue procesar y consolidar la información proveniente de diferentes períodos de tiempo y con formatos distintos. El pipeline ha sido diseñado para manejar datos desde el año 2019 hasta el 2023, permitiendo que se realice un análisis longitudinal que muestra los cambios en la productividad de cultivos de maíz a lo largo del tiempo seleccionado. Este proyecto automatizado facilita el acceso a datos históricos que podrán ser explotados por modelos predictivos y análisis de negocios (BI), esto brinda una visión más profunda sobre las tendencias de rendimiento en el cultivo de maíz.
Identificación del Problema
El proceso manual para descargar datos de productividad de cultivos en la plataforma de Copernicus es bastante ineficiente y tedioso. Para descargar la información, el usuario debe seleccionar manual y repetidamente las variables y los años que le interesan, lo que puede provocar retrasos y una posible pérdida de productividad. Además, aunque Copernicus ofrece una API para agilizar la descarga, esta solo permite descargar la información que previamente se ha seleccionado manualmente en la plataforma.
El pipeline desarrollado en este proyecto automatiza este proceso y permite descargar de una sola vez todos los años y variables necesarias para el análisis de productividad de cultivos, en este caso el maíz. La automatización reduce los tiempos de acceso a la información y mediante la simple edición del código. Es flexible en cuanto a agregar o quitar variables por ejemplo el tipo de cultivo, meses, años o cualquier jerarquía de tiempo de forma dinámica cuando se requiera. El pipeline se puede adaptar fácilmente a los requisitos de cada cliente y en consecuencia que se elimine la dependencia de la selección manual.


Justificación
En la actualidad la agricultura moderna afronta retos como el de maximizar la productividad de los cultivos mientras se gestionan adecuadamente los recursos limitados. Ante esta situación, el acceso rápido y confiable a datos de productividad agrícola es muy importante para poder tomar decisiones relacionadas con la gestión de tierras, recursos y la planificación de cultivos. Sin embargo, la búsqueda manual de esta información puede desembocar en procesos lentos, ineficientes y con probabilidades de error humano, esto potencialmente puede afectar la capacidad de las empresas y entidades agrícolas para tomar decisiones fundamentadas, informadas y justificadas.
Ante esto, el objetivo de este proyecto final de máster tiene solucionar este problema mediante la automatización completa del proceso de descarga, transformación y consolidación de datos de productividad agrícola, específicamente para el cultivo de maíz.

Justificación Técnica
El proceso manual de descarga de datos desde la plataforma de Copernicus es tedioso y se necesita que los usuarios seleccionen las variables y los años uno por uno, lo que puede llevar bastante tiempo cuando se trata de mucha información, al automatizar este proceso, eliminamos la necesidad de la intervención humana, lo que permite ahorrar tiempo y reducir errores. De igual manera permite la escalabilidad y flexibilidad, el pipeline permite que se ajuste fácilmente a futuras expansiones, tanto en términos de las variables agrícolas a analizar como en la incorporación de nuevos cultivos o datos adicionales. Y por último crea una estandarización de procesos, esto nos asegura que los datos sean transformados y almacenados de forma estandarizada, consecuentemente no ayudará a garantizar la integridad de los datos y asegurar que los resultados sean consistentes.

Justificación desde el punto de vista del negocio
Que las empresas puedan acceder a datos actualizados de manera rápida y eficiente permite mejorar la planificación de cultivos, tomar decisiones entendidas sobre el uso de recursos y optimizar las cadenas de suministro. Los retrasos en la obtención de datos debido a la selección manual pueden afectar la capacidad de responder rápidamente a los cambios en el entorno agrícola. Con este pipeline, se elimina esa barrera, lo que reduce el time-to-data y permite una toma de decisiones más ágil. La automatización agiliza el acceso a los datos, y reduce el riesgo de errores al eliminar la necesidad de selección manual. Dicho pipeline permite que los datos descargados sean correctos y mejora la calidad de los posibles futuros análisis que se deseen realizar, todo esto con la certeza que las decisiones basadas en estos datos sean confiables. Esto además puede brindar a las empresas una ventaja competitiva. 

Justificación Académica
Académicamente este proyecto es muy útil porque la agricultura es un campo que cada día se ve más afectado por la tecnología de datos y la inteligencia artificial. He creado este pipeline con el objetivo de mostrar cómo la automatización y el análisis de datos pueden aplicarse para resolver problemas de todo tipo en la agricultura, en este caso enfocado en el maíz. 


Objetivos

Objetivo General
•	El objetivo principal de este proyecto es crear e implementar un pipeline de Extracción, Transformación y Carga de datos que permita la integración automatizada de datos de productividad de cultivos de maíz a lo largo de varios años. Para luego crear una base de datos que contenga toda la información modelada para su posterior análisis y uso en modelos predictivos.
Objetivos Específicos
•	Automatización del proceso ETL: Desarrollar un conjunto de scripts que permitan la extracción de datos de diversas fuentes en este caso de la API de Copernicus, la transformación de los datos en archivos consolidados, y la carga en una base de datos alojada en Amazon S3.
•	Manejo de datos multi-año: Asegurar que el pipeline gestione de forma eficiente los datos de productividad de cultivos de maíz de múltiples años, agrupando la información de los años 2019 al 2023 para su análisis en conjunto.
•	Configuración del pipeline: Crear una configuración donde haya una periodicidad existente cuando el pipeline haga el proceso correspondiente, logrando que los datos se actualicen de forma continua y eficiente, siempre y cuando los datos estén disponibles.
•	Disponibilidad de datos para análisis: Asegurar que los datos procesados y almacenados en Amazon S3 estén en un formato entendible y de fácil acceso para el usuario y que puedan ser utilizados para realizar modelos de machine learning o análisis de Inteligencia Empresarial (BI por sus siglas en inglés).
Arquitectura Técnica
El pipeline desarrollado para este proyecto sigue una arquitectura modular que se compone de varios scripts interconectados, diseñados para realizar la extracción, transformación y carga (ETL) de datos de productividad de cultivos de maíz a lo largo de varios años. Seguidamente se describen los componentes de la arquitectura.

Lenguaje de Programación
Python 3.12: Se utilizó Python como el lenguaje de programación principal debido a su versatilidad, además de ser el lenguaje de programación principal durante el máster. Posee muchas bibliotecas disponibles para el procesamiento de datos y la interacción con APIs. La versión específica utilizada fue Python 3.12.

Componentes

API de Copernicus
Descripción: Es la fuente principal de los datos de la productividad de cultivos de maíz para este proyecto, otorga el acceso mediante un usuario y un key a información con detalles sobre variables como el Desarrollo del Cultivo (DVS), la Producción Total Sobre el Suelo (TAGP), y el Peso Total de Órganos de Almacenamiento (TWSO) todas por sus siglas en inglés. La API posibilita la opción de descargar los datos en formato comprimido .zip para su procesamiento. 
Uso en el pipeline: Es la fuente de los datos, proporcionando la información base sobre la productividad del maíz desde el 2019 al 2023

Amazon S3
Descripción: Es la plataforma principal de almacenamiento en la nube que utilizamos para guardar los archivos descargados de la API y luego almacenar también los archivos transformados después de que se realice el proceso de ETL. Esto nos asegurará que la información que se almacene sea de forma segura e íntegra, además de facilitar su acceso para futuras consultas o análisis.
Uso en el pipeline: Los datos descargados desde la API se almacenan en el S3 en formato .zip, que contienen archivos tipos NC, estos archivos están organizados por año y variable. Luego, todos los archivos y la base de datos consolidada en SQLite, también se suben a S3, permitiendo su acceso remoto.

GitHub Actions
•	Descripción: Se utiliza para automatizar la ejecución del pipeline mediante un cron job, lo que asegura que los datos se actualicen cada semana.
 

Proceso de Extracción (CropProductivityMaize_Extract.py)
•	Descripción: Se encarga de la extracción automática de los datos desde la API. Mediante una conexión que establece con la API, luego envía solicitudes para recibir los datos de productividad del maíz. El proceso se ejecuta para las variables determinadas en el código (DVS, TAGP y TWSO) y años (2019-2023), descargando los archivos comprimidos en formato .zip. Una vez que los datos son descargados, el script organiza los archivos en una estructura basada dichos años y dichas variables.
•	Detalles del Proceso:
o	Automatización: Una ventaja es la automatización de los datos, permite que los datos se descarguen de forma más eficiente y rápida, además evita que el usuario tenga que estar seleccionado manualmente las variables en el sistema de Copernicus.
o	Manejo de Tiempos de Espera: Debido a la cantidad de datos, el tiempo de descarga puede tomar mucho tiempo, por la tanto hemos agregado tiempos de espera en el código para que la descarga no se detenga por falta de respuesta o que alguna solicitud expire.
o	Almacenamiento en S3: Cuando los archivos son descargados, se almacenan en el S3 correspondiente en el bucket asignado trabajofinalmasterucmoscarduran.
 

•	Descripción: El script se encarga de transformar y cargar los datos descargados de la API, este descomprime los archivos .zip y los convierte en archivos CSV que contienen la información de productividad del maíz por año y variable. Durante el proceso de transformación, se eliminan valores nulos, dejando nada más los datos útiles y listos para ser analizados o cargados en una base de datos.
•	Detalles del Proceso:
o	Descompresión de Archivos: Una vez que los archivos .zip han sido descargados desde Amazon S3, el script se encarga de descomprimirlos y extraer los archivos de datos en formato NetCDF (NC). Estos archivos contienen las variables DVS, TAGP y TWSO para cada año.
o	Filtrado de Datos: El script asegura que solo se mantengan los registros que contienen datos válidos (no nulos), eliminando así cualquier dato incompleto o corrupto que pueda afectar el análisis posterior.
o	Generación de Archivos CSV: Después del filtrado, los datos son convertidos en archivos CSV por cada año y variable. Estos archivos contienen columnas estandarizadas (como time, lat, lon, value y variable), variables que serán explicadas más adelante, que permiten un fácil análisis y procesamiento en etapas posteriores.
o	Carga en S3: Finalmente, los archivos CSV son subidos a Amazon S3 para su almacenamiento, organizados por año. Esto facilita su uso en análisis predictivos y modelos de machine learning.
 

SQLite (CreateDatabase.py)
•	Descripción: La base de datos que hemos utilizado es la SQLite, que para términos del proyecto se amolda muy bien a las necesidades, ya que es una base de datos ligera y accesible por de manera fácil y segura. Aquí consolidamos los archivos CSV. Esto permite almacenar los datos en un formato más eficiente para su consulta y análisis, evitando el uso de múltiples archivos CSV dispersos.

•	Uso en el pipeline: La base de datos SQLite es el resultado final del proceso de ETL, donde todos los datos de productividad de cultivos de los diferentes años son almacenados y organizados en tablas separadas. Esta base de datos se almacena en Amazon S3 en formato .db, permitiendo su acceso desde cualquier ubicación y facilitando la integración con herramientas de análisis y modelos predictivos.
 

Formato de los Archivos CSV Generados
Cada archivo corresponde a un año y se estructura con las siguientes columnas:
time: Fecha en la que se registran los datos. Se expresa en formato YYYY-MM-DD.
lat: Latitud de la ubicación del cultivo.
lon: Longitud de la ubicación del cultivo.
value: Valor de la variable seleccionada.
variable: Indica la variable correspondiente (DVS, TAGP o TWSO).

 
Flujo de Trabajo o Pipeline de Datos. (CropProductivityMaize_pipeline.yml)
1.	Extraer Datos: Para la extracción de datos el pipeline comienza con la ejecución del script CropProductivityMaize_Extract.py, luego se conecta a la API y descarga los archivos comprimidos .zip estos archivos tiene los datos de productividad de maíz en formato NC. Estos archivos se guardan por año y variable en el S3. 
2.	Transformar y filtrar: Una vez que ya se descargaron los archivos, el código los descomprime y los transforma en archivos CSV mediante el script CropProductivityMaize_TransformAndLoad.py. Durante este proceso, se filtran los datos no válidos y se genera un conjunto limpio de archivos CSV que se vuelven a almacenar en S3.
3.	Consolidar Datos: El script CreateDatabase.py crea archivos formato CSV consolidados en una base de datos SQLite, que luego se sube automáticamente al S3. 
4.	Automatización y Actualización con Periodicidad: Para la automatización y la periodicidad usamos “GitHub Actions” El pipeline ejecuta los scripts de manera automática todos los lunes a medianoche y sin necesidad de estar corriendo el proceso manualmente.

 
El proyecto utiliza tres variables clave relacionadas con la productividad de cultivos de maíz. Estas variables son extraídas de la API de Copernicus y representan diferentes aspectos del desarrollo y rendimiento de los cultivos.
1. DVS (Crop Development Stage)
•	Descripción: La variable DVS (Etapa de Desarrollo del Cultivo) indica la fase fenológica del cultivo de maíz. Representa cómo ha progresado el desarrollo del cultivo desde su siembra hasta la maduración descritos por de (Wit, 2024)
•	Valores:
o	0.0: Representa la emergencia del cultivo, es decir, cuando las plantas recién han comenzado a crecer.
o	1.0: Indica el estado de floración (anthesis), cuando el cultivo está en su etapa reproductiva y se produce la fertilización.
o	2.0: Representa la madurez fisiológica, cuando el cultivo ha alcanzado su madurez completa y está listo para ser cosechado.
•	Importancia: Esta variable es fundamental para entender en qué etapa del ciclo de vida se encuentra el cultivo, lo cual influye directamente en las decisiones de manejo y cosecha.
2. TAGP (Total Above-Ground Production)
•	Descripción: La variable TAGP (Producción Total Sobre el Suelo) mide la cantidad total de materia seca producida por el cultivo, expresada en kilogramos por hectárea (kg/ha). Incluye todas las partes del cultivo que crecen sobre el suelo, como los tallos, hojas y granos. (de Wit, 2024).
•	Valores: Esta variable puede tomar diferentes valores numéricos, generalmente en el rango de cientos a miles de kilogramos por hectárea, dependiendo de la eficiencia del cultivo en capturar luz solar y convertirla en biomasa.
•	Importancia: La producción total sobre el suelo es un indicador clave de la salud y productividad del cultivo, y se utiliza para estimar el rendimiento total del campo.
3. TWSO (Total Weight of Storage Organs)
•	Descripción: La variable TWSO (Peso Total de los Órganos de Almacenamiento) mide el peso seco de los órganos de almacenamiento del cultivo, como los granos en el caso del maíz. Al igual que TAGP, se expresa en kilogramos por hectárea (kg/ha). (de Wit, 2024).
•	Valores: Los valores de esta variable varían según el rendimiento del cultivo, representando la cantidad de producto cosechable en condiciones óptimas.
•	Importancia: El TWSO es un parámetro importante para los agricultores, ya que representa la parte comercializable del cultivo. Un valor alto de TWSO generalmente se traduce en mayores ingresos para el agricultor.
Descripción de las Librerías Utilizadas
Durante el desarrollo de este pipeline, hemos utilizado diversas librerías de Python que cumplen funciones específicas dentro del proceso de extracción, transformación y carga de datos. En este apartado explicaremos brevemente la función de cada librería.
1. boto3
•	Descripción: Es la librería oficial de Amazon Web Services (AWS) para interactuar con servicios como S3.
•	Uso en el proyecto: Se utiliza para subir y descargar archivos desde y hacia Amazon S3, donde se almacenan los datos de productividad de cultivos descargados y procesados.
2. cdsapi
•	Descripción: Es la librería que proporciona acceso a la API del Servicio de Cambio Climático de Copernicus.
•	Uso en el proyecto: Se utiliza para extraer datos de productividad agrícola desde la API de Copernicus, permitiendo la automatización de la descarga de variables como DVS, TAGP, y TWSO. Para acceder a esta librería hay que tener una sesión creada en Copernicus y que se asignen un usuario y un key secreto.
3. xarray
•	Descripción: Es una de las librerías principales del proyecto, se utiliza para trabajar con grandes conjuntos de datos multidimensionales, como los archivos NetCDF.
•	Uso en el proyecto: Se emplea para leer y procesar los archivos NetCDF descargados de la API de Copernicus.
4. pandas
•	Descripción: Una librería básica de Python, es esencial y clave para la transformación de datos.
•	Uso en el proyecto: La utilizamos para transformar los datos descargados en formatos como CSV, y para filtrar y organizar los datos en Dataframes, facilitando la conversión y consolidación de la información.
5. sqlite3
•	Descripción: Es la librería que permite interactuar con bases de datos SQLite.
•	Uso en el proyecto: La usamos para crear y almacenar los datos de productividad en una base de datos SQLite consolidada.
6. tempfile
•	Descripción: La librería que estamos utilizando para generar archivos temporales
•	Uso en el proyecto: Se utiliza para crear archivos temporales donde se descargan y almacenan los datos antes de ser procesados y subidos a Amazon S3.
7. zipfile
•	Descripción: Librería que usamos para trabajar con archivos en formato ZIP.
•	Uso en el proyecto: Se emplea para descomprimir los archivos descargados en formato ZIP desde la API de Copernicus, antes de procesarlos en formato CSV.
8. matplotlib y seaborn
•	Descripción: Librerías esenciales de Python para la creación de gráficos y visualizaciones.
•	Uso en el proyecto: Aunque en este proyecto, en el github no realizamos gráficos directos, estas librerías si las utilizamos para hacer un ejemplo de modelado en jupyter. (Ver Anexos).
9. python-dotenv
•	Descripción: Facilita la carga de variables de entorno desde un archivo. env.
•	Uso en el proyecto: Se utiliza para cargar de manera segura las credenciales de AWS y las claves de acceso a la API de Copernicus, asegurando que estas no estén hardcodeadas como se dice en el argot popular “quemadas” en el código fuente.

Caso de Uso de Negocio
Contexto
La agricultura es un sector muy importante en la vida del ser humano y su cadena de alimentación desde tiempos antiguos, la productividad agrícola es clave para la toma de decisiones en múltiples sectores, desde la planificación gubernamental hasta la gestión de recursos en empresas agrícolas. 
El acceso a información precisa y actualizada sobre la productividad de cultivos es fundamental para realizar predicciones, identificar tendencias y tomar decisiones estratégicas (FAO, 2019). Sin embargo, obtener estos datos de forma manual puede ser un proceso lento, propenso a errores y que demanda una inversión significativa de tiempo. En el caso de la productividad de cultivos, Copernicus ofrece una plataforma para la obtención de datos agrícolas basados en observación satelital, proporcionando información como la etapa de desarrollo del cultivo (DVS), la producción total (TAGP) y el peso de los órganos de almacenamiento (TWSO) (de Wit, 2024). Sin embargo, el proceso manual de descarga y selección de variables puede generar retrasos considerables y una sobrecarga de trabajo para los analistas o técnicos que requieren dichos datos con frecuencia.
Problema Por Resolver
El problema principal que este pipeline busca solucionar es la fragmentación y la ineficiencia en la obtención de datos agrícolas históricos y actualizados a través de la plataforma de Copernicus. El proceso manual de descarga requiere que el usuario seleccione variables específicas (DVS, TAGP, TWSO) y años uno por uno, lo que genera retrasos y una mayor complejidad operativa. A medida que aumenta el número de variables o años que se desean consultar, este enfoque manual se vuelve aún más lento y difícil de gestionar.
Además, aunque Copernicus ofrece una API que facilita la descarga de datos, esta API está limitada a las variables que han sido seleccionadas manualmente. Esto significa que, cada vez que un usuario necesita obtener datos nuevos o actualizar los existentes, debe realizar el proceso manual nuevamente, lo cual puede generar retrasos en la toma de decisiones y afectar la productividad del análisis.
 
Solución Propuesta
El pipeline desarrollado automatiza el proceso de descarga y consolidación de los datos, eliminando la necesidad de seleccionar manualmente las variables y los años cada vez que se desean obtener datos. En su lugar, el pipeline descarga todos los datos necesarios de una sola vez, consolidando los archivos descargados en un formato listo para ser analizado. Las principales características de la solución son:
1.	Automatización Completa: El pipeline está configurado para ejecutarse de manera automática cada semana, descargando los datos de productividad de cultivos para las variables y años predefinidos sin intervención manual.
2.	Flexibilidad y Escalabilidad: Si en algún momento se necesitan agregar nuevas variables o modificar el rango de años a consultar, esto se puede hacer fácilmente editando la configuración del pipeline. Esta flexibilidad permite que la solución se adapte rápidamente a las necesidades cambiantes del negocio.
3.	Consolidación de Datos: El pipeline unifica los datos de productividad agrícola en una base de datos estructurada, lo que facilita su consulta y uso en modelos de machine learning o análisis de Inteligencia Empresarial (BI por sus siglas en inglés). Esta base de datos está disponible en la nube a través de Amazon S3, lo que asegura su accesibilidad desde cualquier ubicación.
4.	Eliminación de Retrasos: Gracias a la automatización del proceso y la eliminación de la necesidad de selección manual, se reducen significativamente los tiempos de acceso a los datos, lo que permite que las decisiones basadas en estos datos se tomen de manera más rápida y precisa.
Mejoras Introducidas por la Solución
Comparado con el proceso manual existente, esta solución introduce varias mejoras clave:
1.	Reducción de Tiempo: El proceso manual de selección y descarga puede llevar horas, especialmente cuando se deben manejar grandes volúmenes de datos o cuando se trabaja con múltiples variables. Con este pipeline, el proceso se reduce a minutos y no requiere intervención humana.
2.	Precisión y Consistencia: Al automatizar el proceso, se elimina la posibilidad de errores humanos en la selección de variables o años, garantizando que los datos descargados sean consistentes y completos cada vez que el pipeline se ejecuta.
3.	Acceso Centralizado a los Datos: Los datos descargados son organizados y almacenados en una base de datos única, lo que facilita su acceso y consulta. En lugar de trabajar con múltiples archivos dispersos, los usuarios pueden acceder a los datos consolidados de manera rápida y eficiente.
4.	Facilidad de Expansión: En el futuro, si se requiere la integración de nuevas variables o datos adicionales, el pipeline puede ser modificado fácilmente para adaptarse a estos cambios sin necesidad de rediseñar todo el sistema.
Alternativas Existentes
Antes de la implementación de este pipeline, las alternativas disponibles para obtener datos de productividad agrícola eran las siguientes:
1.	Descarga Manual desde la Plataforma de Copernicus: La opción tradicional consistía en que el usuario navegara por la plataforma de Copernicus y seleccionara las variables y años manualmente para descargar los datos. Esto resultaba en un proceso engorroso, especialmente cuando se trataba de analizar grandes volúmenes de datos.
2.	Uso Básico de la API de Copernicus: Si bien la API de Copernicus permite la descarga de datos de manera programática, está limitada a las selecciones manuales realizadas previamente. Esto implica que, cada vez que se desea actualizar los datos o seleccionar nuevos parámetros, es necesario realizar el proceso manual en la plataforma.
Impacto en el Negocio
La implementación de este pipeline tiene un impacto directo en la eficiencia operativa de la empresa que desee acceder a los datos de productividad agrícola. Al reducir significativamente los tiempos de obtención de datos y aumentar la precisión, las decisiones basadas en datos pueden ser tomadas de manera más informada y rápida, lo que se traduce en mayor competitividad y mejora en la planificación agrícola.
Conclusiones
El desarrollo de este pipeline para la automatización de la descarga, transformación y consolidación de datos de productividad agrícola representa una solución eficiente y escalable que resuelve los problemas asociados con la obtención manual de datos. A lo largo de este proyecto, se ha demostrado que una arquitectura bien diseñada y automatizada puede mejorar significativamente el acceso a datos críticos para la toma de decisiones, permitiendo a las empresas y entidades agrícolas reducir tiempos y mejorar la precisión en sus análisis.
Además, este pipeline no solo aborda la eficiencia operativa, sino que también crea nuevas oportunidades para el análisis avanzado. Al consolidar datos históricos de múltiples años, se facilita la implementación de modelos de minería de datos y machine learning que permiten realizar predicciones de rendimiento, identificar tendencias a largo plazo y ajustar estrategias agrícolas en función de condiciones cambiantes. Este enfoque automatizado no solo reduce la intervención humana, sino que potencia la capacidad de las empresas para tomar decisiones basadas en datos y optimizar sus operaciones agrícolas con mayor precisión y rapidez.
Desafíos y Lecciones Aprendidas
Aprendiendo GitHub desde Cero
Antes de este proyecto, nunca había utilizado GitHub en mi vida. Estos meses de trabajo no solo se dedicaron al proyecto en sí, sino también a familiarizarme y aprender a utilizar nuevas herramientas. GitHub fue una de las principales, por lo que tuve que comenzar investigando cómo funcionaba, creando una cuenta nueva y entendiendo los conceptos básicos. Por ejemplo, descubrí que incluso para crear archivos de Python debía poner el sufijo .py al final. Este proceso fue fundamental para estructurar adecuadamente el pipeline de datos.

El Origen Orgánico del Proyecto
El tema de este proyecto nació de forma orgánica. Inicialmente, buscaba bases de datos para la primera opción del proyecto, que estaba más alineada con el rol de data scientist. Sin embargo, al ver que la información solo podía descargarse año por año en la plataforma de Copernicus, pensé en crear una solución que permitiera descargar múltiples años y así construir series temporales para minería de datos y modelización predictiva. Me reuní con un colega, experto en la creación de pipelines de datos, quien me guió sobre cómo podríamos consolidar todos los años usando la API de Copernicus. Así fue como nació este proyecto, con el objetivo de automatizar la obtención y disponibilidad de datos a través de los años seleccionados, además de permitir que los datos fueran potencialmente editables desde el código.
Desafíos con Amazon S3
Otro gran desafío fue la integración con Amazon S3. Aprender a crear los secretos en GitHub para conectar el pipeline con S3 y almacenar los datos en la nube fue un reto significativo, ya que nunca había utilizado S3 para este propósito. Tuve que aprender nuevas librerías y desarrollar código que almacenara los datos en S3, lo cual fue una experiencia demandante pero enriquecedora. Recuerdo que hice muchos intentos de prueba y error, apoyándome en el conocimiento adquirido durante el máster.
Construcción de la Base de Datos
Crear una base de datos a partir de los archivos CSV fue otra lección importante. Aunque el código en sí es relativamente sencillo, establecer la conexión de los datos a un archivo .db fue un gran aprendizaje durante el proceso del trabajo final del máster (TFM). Pude integrar Power BI y proporcionar la posibilidad de descargar la base de datos localmente para usarla según las necesidades del usuario.
Exploración de Librerías Nuevas: xarray y boto3
Estudiar la sintaxis y comprender cómo funcionan las librerías xarray y boto3 fue una parte crucial del proceso, y me tomó bastante tiempo. Estas herramientas resultaron ser extremadamente útiles para el procesamiento de los datos y la interacción con la nube.
Dedicación Personal y Emocional
Este proyecto tiene una dedicación muy especial. Mi padre falleció hace exactamente un año. Antes de su inesperado fallecimiento, él supo que fui admitido en el máster en Big Data, Data Science e Inteligencia Artificial en la Complutense, y eso lo llenó de alegría. Esa felicidad la proyectaba cada vez que lograba solucionar un problema en el código o cuando conseguía realizar la conexión entre los sistemas explicados. Mi padre fue ingeniero en informática y desde que yo era niño él me inculcó el amor por esta materia. Por eso y muchas cosas más, este proyecto está dedicado a él.
Esfuerzo en Medio de Grandes Cambios
Fue un trabajo arduo, ya que utilicé principalmente fines de semana para avanzar, ya que tengo un trabajo a tiempo completo como analista de datos, el cual comencé hace cuatro meses. Fueron meses de muchos cambios, donde tuve que salir de mi zona de confort y por ende ha sido una etapa de mucho crecimiento personal y profesional.
De Consumidor a Creador de Datos
Durante mi carrera profesional, siempre he sido consumidor de bases de datos ya hechas o datos ya consolidados. Sin embargo, este proyecto me dio la oportunidad de crear estos datos por mi cuenta. Aunque mi experiencia siempre había estado más ligada al "front-end", con la visualización de datos, el "back-end" fue algo nuevo para mí. Estoy muy agradecido con la Complutense y los profesores por su valioso aporte a mi carrera profesional y académica.
La Importancia de la Automatización y la Integridad de los Datos
Aprendí mucho sobre la importancia de la automatización y la integridad de los datos. Uno de los procesos más complicados fue el de poner la información de Copernicus en el formato correcto. Los datos venían en archivos NC, y convertirlos a CSV trajo consigo bastantes desafíos, especialmente para asegurarse de que estuvieran en un formato adecuado y fácil de usar para el usuario. Afortunadamente, después de muchos esfuerzos, se logró.
Satisfacción y Nuevas Oportunidades
Estoy muy contento con el resultado final de mi proyecto y satisfecho con todo el camino recorrido. Tanto así, que el colega que me guió inicialmente ya me ha contactado para realizar trabajos profesionales de creación de pipelines para una empresa en Lituania.
Reconocimiento Profesional
Gracias a este proyecto, he podido darme a conocer en la comunidad de científicos de datos en Costa Rica, lo que ha abierto nuevas oportunidades profesionales y de crecimiento en mi carrera.



Bibliografía y Referencias
•	de Wit, A. (2024). Crop Productivity Indicators: Product User Guide and Specification (PUGS). Climate Data Store Copernicus. Recuperado el 15 de septiembre de 2024, de https://confluence.ecmwf.int/pages/viewpage.action?pageId=277352518

•	FAO. (2019). El estado mundial de la agricultura y la alimentación 2019: Avances en la reducción de la pérdida y el desperdicio de alimentos. Organización de las Naciones Unidas para la Alimentación y la Agricultura. Recuperado el 3 de agosto de 2024, de http://www.fao.org/3/ca6030es/ca6030es.pdf

•	World Bank Group. (2020). Agricultural Productivity and Innovation. Recuperado el 16 de agosto de 2024, de https://www.worldbank.org

•	Copernicus Climate Data Store. (2024). Crop Productivity Indicators: Dataset Overview. Recuperado el 8 de junio de 2024, de https://cds-beta.climate.copernicus.eu/datasets/sis-agroproductivity-indicators?tab=overview

Links Importantes
Descarga manual de la información de Copernicus:
https://cds-beta.climate.copernicus.eu/datasets/sis-agroproductivity-indicators?tab=download
Repositorio en GitHub con el código del proyecto:
https://github.com/oskr1403/Trabajo-Final-del-Master-UCM-OscarDuran
Carpeta del pipeline en GitHub:
https://github.com/oskr1403/Trabajo-Final-del-Master-UCM-OscarDuran/tree/main/.github/workflows
Carpeta de los scripts de extracción, transformación y carga, y creación de la base de datos:
https://github.com/oskr1403/Trabajo-Final-del-Master-UCM-OscarDuran/tree/main/App
Descarga de la base de datos consolidada en formato .db desde S3:
https://trabajofinalmasterucmoscarduran.s3.us-east-2.amazonaws.com/processed_data/crop_productivity.db

Agradecimientos

•	En primer lugar, quiero expresar mi más sincero agradecimiento a Dios, mi Padre y Todopoderoso, quien me da la vida cada día y me ha brindado la fortaleza para superar todos los desafíos que este camino ha presentado.

•	A mi padre, Mainor Durán Blanco, que en paz descanse, quien siempre creyó en mí y supo, antes de partir, que iniciaría esta maestría. Su alegría y orgullo por este logro me han acompañado en cada paso de este recorrido, y es a él a quien dedico este proyecto.

•	Quiero agradecer profundamente a mi madre, Janilu Azofeifa Valverde, y a mis hermanas, Daniela Durán Azofeifa y Carolina Durán Azofeifa, quienes me han brindado un apoyo incondicional a lo largo de estos meses. Su amor y comprensión han sido un pilar fundamental en mi vida personal y académica.

•	Mi más sincero agradecimiento a mi mentor, Giovanni Solano Porras, por su guía y apoyo continuo en todo este proceso. Su orientación ha sido clave en el desarrollo de este proyecto.

•	También quiero reconocer el aporte invaluable de cada uno de los profesores y tutores de los cursos y del Trabajo Final de Máster. Su dedicación y compromiso me han permitido adquirir las herramientas necesarias para alcanzar este logro.

•	A VMware, agradezco por el apoyo financiero a través de su plan de subsidio de carrera, que me permitió afrontar los retos económicos de esta maestría y concentrarme en mi desarrollo profesional.

•	Finalmente, agradezco a todos los que, de una forma u otra, han sido parte de este camino. Estoy profundamente agradecido por cada consejo, cada palabra de aliento y cada oportunidad de crecimiento que me han brindado





