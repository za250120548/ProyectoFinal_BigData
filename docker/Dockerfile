# Usa la imagen oficial de Airflow como plantilla.
FROM apache/airflow:2.10.2

# Cambia a usuario airflow para instalar paquetes.
USER airflow

# Copia el archivo de requerimientos al contenedor (ruta relativa al build context - raíz del repo).
# El build context se estableció en la raíz del proyecto, por eso referenciamos la ruta completa:
COPY docker/requirements.txt .

# Instala todas las dependencias del archivo.
RUN pip install -r requirements.txt

# Copia el directorio dbt al contenedor (ruta relativa al build context).
# Cuando el build context se establece en la raíz del proyecto, podemos usar:
COPY dbt /opt/airflow/dbt