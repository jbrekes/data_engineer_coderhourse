# Proceso: Rent Analysis ETL Docker

Esta imagen te permite ejecutar un proceso de web scraping para extraer datos de propiedades listadas en [Argenprop](https://www.argenprop.com/) en la Ciudad Autónoma de Buenos Aires.

## Prerequisitos

1. Es necesario tener instalados Docker y Docker Compose
2. El [código del proceso ETL](https://github.com/jbrekes/data_engineer_coderhourse/blob/main/entregables/rent_analysis_docker/rent_analysis_etl.py) da por sentado que ya disponemos de una tabla creada en Redshift con los campos requeridos. Puedes revisar el código para evaluar qué columnas debe tener dicha tabla

## Cómo Correr la Imagen

**Paso 1:** Clona este repositorio en tu máquina.

**Paso 2:** Abre la terminal y navega al directorio donde se encuentra el repositorio.

**Paso 3:** Crea la imagen de Docker con el siguiente comando, reemplazando `[nombre-de-tu-imagen]` por el nombre que desees para la imagen:

```bash
docker build -t [nombre-de-tu-imagen] .
  
```

**Paso 4:** Ejecuta la imagen de Docker para iniciar el proceso de ETL. Asegúrate de configurar las variables de entorno necesarias para la conexión con Redshift. Utiliza un comando similar al siguiente:
```bash
docker run --name [nombre-de-tu-contenedor] \
  -e REDSHIFT_HOST= [CODERHOUSE-REDSHIFT-HOST] \
  -e REDSHIFT_PORT= [PUERTO] \
  -e REDSHIFT_DATABASE= [REDSHIFT-DATABASE] \
  -e REDSHIFT_USER= [USER] \
  -e REDSHIFT_PASSWORD= [CONTRASEÑA] \
  -e REDSHIFT_SCHEMA= [ESQUEMA] \
  -e REDSHIFT_TABLE= daily_house_rent_listings \
  -d [nombre-de-tu-imagen]
```

Este proceso te permitirá ejecutar con facilidad el web scraping y la extracción de datos para el análisis de alquileres.

¡Disfruta de tu proceso de ETL con Docker!
