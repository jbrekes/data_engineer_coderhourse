# Configuración de Rent Analysis con Airflow

## Introducción
Este tutorial te guiará a través de los pasos necesarios para configurar un DAG (Directed Acyclic Graph) en Apache Airflow. Utilizaremos Docker para ejecutar Airflow, y dentr de éste crearemos una función `rent_analysis_etl`. Sigue estos pasos para poner en marcha tu flujo de trabajo de análisis de alquiler.

## Paso 1: Instalación de Airflow
Sigue las instrucciones de las clases de Coderhouse para instalar Apache Airflow utilizando Docker. 

## Paso 2: requirements.txt
En el directorio donde se encuentra el archivo `docker-compose.yaml`, crea o copia el siguiente archivo [requirements.txt](https://github.com/jbrekes/data_engineer_coderhourse/blob/main/airflow_docker/requirements.txt) que contiene las bibliotecas necesarias para el proyecto.

## Paso 3: Crear una nueva imagen de Airflow
Crea una nueva imagen de Airflow basada en la imagen original, pero con las bibliotecas del archivo `requirements.txt`. Puedes utilizar el siguiente [Dockerfile](https://github.com/jbrekes/data_engineer_coderhourse/blob/main/airflow_docker/dockerfile) como base. 

En la consola, desde el directorio de trabajo, crea la nueva imagen utilizando el comando:
```bash
docker build . --tag pyrequire_airflow:2.3.3
```

## Paso 4: Actualizar docker-compose.yaml
Abre el archivo [docker-compose.yaml](https://github.com/jbrekes/data_engineer_coderhourse/blob/main/airflow_docker/docker-compose.yaml). En la línea 48, donde se especifica el nombre de la imagen de Docker, reemplázala por el nombre de la nueva imagen que creaste. La línea debería verse similar a:
```yaml
image: ${AIRFLOW_IMAGE_NAME:-pyrequire_airflow:2.3.3}
```

## Paso 5: Archivo Python
Asegúrate de tener, dentro de la carpeta `dags`, un nuevo directorio llamado "rent_analysis_functions" con el siguiente [archivo python](https://github.com/jbrekes/data_engineer_coderhourse/blob/main/airflow_docker/dags/rent_analysis_functions/functions.py), el cual contiene la función "etl_process" que es la que realizará todo el proceso.

## Paso 6: Levantar Docker
Inicia Docker usando el comando:
```bash
docker-compose up -d
```

## Paso 7: Configurar Variables de Airflow
Desde la UI de Airflow, ve al apartado de Admin -> Variables y crea las siguientes variables

#### rent_analysis_settings
Contiene los datos necesarios para conectarnos a la base de datos de Redshift. Será un diccionario con los siguientes valores (reemplaza el usuario y contraseña por los propios):
```json
{
  "REDSHIFT_HOST": "CODERHOUSE_REDSHIFT_HOST",
  "REDSHIFT_PORT": "5439",
  "REDSHIFT_DATABASE": "data-engineer-database",
  "REDSHIFT_USER": "YOUR_USER",
  "REDSHIFT_PASSWORD": "YOUR_PASSWORD",
  "REDSHIFT_SCHEMA": "jbrekesdata_coderhouse",
  "REDSHIFT_TABLE": "daily_house_rent_listings"
}
```

#### listings_preferences
Contiene un diccionari de preferencias de búsqueda. Esto permitirá que se filtren los registros agregados a la base de datos para generar y enviar un email personalizado en el que se contendrán los registros que complan con los requisitos especificados. Esto será útil si queremos estar al tanto de nuevos departamentos en alquiler que cumplan con nuestras necesidades. Por el momento, sólo se agregaron como parámetros el **número de baños, dormitorios, y superficie total en m2**, pero fácilmente podrá editarse el código para incluir nuevos criterios.

Para esto, deberán agregarse las variables en el diccionario mencionado, y editar el filtro de la función [send_daily_email](https://github.com/jbrekes/data_engineer_coderhourse/blob/main/airflow_docker/dags/rent_analysis_functions/functions.py) para contemplar estos nuevos valores.

## Paso 8: COnfigurar las variables SMTP de Airflow
Podrán editarse tanto en el archivo airflow.cfg, como dentro del archivo docker-compose.yml (se optó por esta segunda opción en este caso). Esto permitirá hacer uso del servicio de envío de email de Gmail.

```
    AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
    AIRFLOW__SMTP__SMTP_PORT: 587
    AIRFLOW__SMTP__SMTP_STARTTLS: True
    AIRFLOW__SMTP__SMTP_SSL: False
    AIRFLOW__SMTP__SMTP_USER: TU-USUARIO
    AIRFLOW__SMTP__SMTP_PASSWORD: TU-CONTRASEÑA
    AIRFLOW__SMTP__SMTP_MAIL_FROM: TU-USUARIO
```

Cabe destacar que la contraseña a agregar en este caso no es la misma que utilizamos diariamente en nuestra cuenta, sino que debemos generar una nueva App Password. Puedes ver un instructivo [aquí](https://knowledge.workspace.google.com/kb/how-to-generate-an-app-passwords-000009237?hl=es-419) 

## Paso 8: Ejecutar la tarea
Con los pasos anteriores, ya deberías estar en condiciones de correr la task sin problemas.
