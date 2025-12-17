# Proyecto final   

## Descripción del proyecto
Este proyecto emula la  obtencion, extraccion, carga y procesamiento de datos de telemetria de una pequeña flota de vehiculos
Cada vehiculo cuenta con un numero de identificador vehicular VIN unico, el nombre del OEM, y cada vehiculo comparte datos de su ubicaciòn con longitu y latitud, la velicidad, el odomero y la fecha y hora en que los datos se obtien

## Toolchain Principal
1. Docker
2. Python
3. Airflow
4. PosgressSQL
5. Faker
6. Polars

## Generacion de datos

La genarición de datos se hace de manera sintetica mediante la libreria faker de python, donde se generan números aleatorios de datos para longitud, latitud, velicidad, odometro, fecha y hora.

Para hacer una ejemplificacion mas fiel a un caso real se genera una lista vehiculos que forman la flotilla.
Cada vehiculo debe contar con caractaristicas como el fabricante, modelo y un odometro inicial.


    fleet_list = [
    {"VIN":"F654J5DV77JS98654", "OEM":"Ford", "model_year":2023, "odometer":22106},
    {"VIN":"F654J5DV77JS46566", "OEM":"Ford", "model_year":2024, "odometer":19615},
    {"VIN":"3GCUKREC0FG123456", "OEM":"Chevrolet", "model_year":2024, "odometer":3999},
    {"VIN":"3GCUKREC0FG893480", "OEM":"Chevrolet", "model_year":2025, "odometer":3925},
    {"VIN":"1V2NC9JX5JC123456", "OEM":"Volvo", "model_year":2022, "odometer":13560},
    {"VIN":"1V2NC9JX5JC962225", "OEM":"Volvo", "model_year":2022, "odometer":11050},
    {"VIN":"1VWAP7A30JC123456", "OEM":"Volkswagen", "model_year":2021, "odometer":20000},
    {"VIN":"WDBUF56X98B123456", "OEM":"Mercedes-Benz", "model_year":2017, "odometer":40000},
    {"VIN":"3AKJGLD54JSC12345", "OEM":"Freightliner", "model_year":2022, "odometer":10000},
    {"VIN":"6FDUF4GY1JSC12345", "OEM":"Scania", "model_year":2016, "odometer":50000},
    {"VIN":"1XKAD49X7JJ123456", "OEM":"Kenworth", "model_year":2015, "odometer":60000},
    {"VIN":"LZB12345678901234", "OEM":"FAW", "model_year":2023, "odometer":8000}
    ]

Con la libreria faker se elije un vehiculo al azar para extraer sus caracteristicas y luego generar los datos sintéticos.
Esto se hace en la funcion
    def _generate_record(fake: Faker) -> list:

El sistema simula los datos enviados por la flotilla de vehiculos que se concentran en un solo archivo ubicado en 
opt/airflow/data/raw_data.csv, este archivo crece agregando mas datos de telemetria generados aleatoriamente.

# Limitantes

Los datos generados no se revisados por coherencia, ya que no es el principal objetivo del proyecto, y una mejora substancial es que el odometro generado siempre tiene que ser mayor que el anterior pues odometro de los vehiculos es un contador monotonica que solo incrementa.

Otro dato que se ignora la coherencia es el dato de geolocalización pues las cordenadas no siguen un patrn y pudieran estas e cientos o miles de kilometros en un corto tiempo, respecto al time stampes position.

## Diseño de la base de datos

El diseño de la base detos se hizo con draw.io y se puede consultar en 
\ProyectoFinal_BigData\data\dbc_diagram.drawio

La base de datos diseñada consta de 4 tablas, la table 2,3 y 4 se generan en la etapa plata (staging) del workflow ELT, en la etapa oro se oculta informacion sensible con "*", el argumento detras es que solo los algunos ejecutivos y administradores de la DBC tienen acceso a los datos curdos (bronce) y staggin (Plata) donde toda la información esta disponible VIN, Geo position etc.
1. Telemetry
2. Vehicle
3. Veh_speeds
4. LastPos

### Telemetry
Esta tabla contiene todo los datos de telemetria de todos los vehiculos de la flotilla,

### Vehicle
Esta table se genera a partir de la tabla Telemetry, obteniendo todos los vehiculos que hayan subido al menos un data de telemetria, ya que el VIN de un vehiculo debe ser único se agrupa todos los datos de telemetria generadas para asi obtener la lista de vehiculos de la flotilla de modo que la cantidad de vehiculos se actualiza automaticamente.

### VehAvgSpeed
Esta table tambien se genera automaticamente y se genera un nuevo dato que es el promedio de la velocidad de cada uno de los vehiculos de la flotilla, este dato se genera agrupando por VIN y luego sacando el promedio la velocidad de cada vehiculo

### LastPos
Esta tabla se genera automaticamente y extrae la posicion mas reciente rportada por cada vehiculo en la tabla de Telemetry



## Archivos claves

#### data/fleet_raw_data.csv
Archivo que contiene los dato sinteticos.

#### data/dbc_diagram.drawio
Diseño de tabla y datos de la base de datos.

#### docker/docker-compose.yaml
Archivo que configura el cluster de Docker.

#### docker/Docker file
Archivo de docker

#### dags/driven_data_pipeline.py
Implementacion del DAG consus tareas.

### docs/EvidenceProjectoIntegradorFinal.pdf
Presentacion de powerpoint con las evidencias del proyecto.
Aqui se podra encontrar la evidencia y el reporte de la practica

#### docker/requirements.txt
Archivo de bibliotecas necesarias para la creacion de la imagen de docker y el correcto funcionamiento

## Documentacion y reporte
Toda la documentacion y reporte de la practica final se encuentra en 
docs/EvidenceProjectoIntegradorFinal.pdf
