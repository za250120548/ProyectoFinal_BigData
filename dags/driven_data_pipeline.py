import csv
import random
import logging
import uuid
import polars as pl

from faker import Faker
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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


""" Create a list of Vehicle objects from the fleet_list data """
def create_fleet_objects(fleet_data: list) -> list:
    #Fleet of vehicles with predefined VINs, OEMs, model years, and Initial odometer readings.
    fleet_objects = []
    for vehicle_data in fleet_data:
        vehicle = Vehicle(
            VIN=vehicle_data["VIN"],
            OEM=vehicle_data["OEM"],
            model_year=vehicle_data["model_year"],
            odometer=vehicle_data["odometer"]
        )
        fleet_objects.append(vehicle)
    return fleet_objects

# Configure logging.
logging.basicConfig(
    level=logging.INFO,                    
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)


""" Crete a vehicle class with attributes such as VIN, OEM, model, year and odometer
The class shall return a dictionary with the vehicle attributes
and method to convert the object to a dictionary and to return the vehicle attributes.
use faker to generate the vehicle VIN, model_year and odometer value.
"""
class Vehicle:
    def __init__(self, VIN: str, OEM: str, model_year: int, odometer: int):
        self.VIN = VIN
        self.OEM = OEM
        self.model_year = model_year
        self.odometer = odometer

    def to_dict(self) -> dict:
        return {
            "VIN": self.VIN,
            "OEM": self.OEM,
            "model_year": self.model_year,
            "odometer": self.odometer
        }

    def get_vehicle_info(self) -> dict:
        return self.to_dict()
    
    def get_VIN(self) -> str:
        return self.VIN

    def get_OEM(self) -> str:
        return self.OEM

    def get_model_year(self) -> int:
        return self.model_year

    def get_odometer(self) -> int:
        return self.odometer

    def update_odometer(self, increment) -> None:
        self.odometer += increment

    ''' desctructor method'''
    def __del__(self):
        del self



def _create_data(locale: str) -> Faker:
    """
    Creates a Faker instance for generating localized fake data.
    Args:
        locale (str): The locale code for the desired fake data language/region.
    Returns:
        Faker: An instance of the Faker class configured with the specified locale.
    """
    # Log the action.
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)

def _get_random_vehicle() -> Vehicle:
    """
    Returns a random Vehicle object from the predefined fleet list.
    Returns:
        Vehicle: A randomly selected Vehicle object.
    """
    vehicle_data = random.choice(fleet_list)
    vehicle = Vehicle(
        VIN=vehicle_data["VIN"],
        OEM=vehicle_data["OEM"],
        model_year=vehicle_data["model_year"],
        odometer=vehicle_data["odometer"]
    )
    return vehicle
    

def _generate_record(fake: Faker) -> list:
    """
    Generates a single fake user record.
    Args:
        fake (Faker): A Faker instance for generating random data.
    Returns:
        list: A list containing various fake fleet of transport vehicles such as VIN, speed, Driving hours, 
    """
    # Generate random Vehicle Data.
    vehicle = _get_random_vehicle()
    VIN = vehicle.get_VIN()  # Generate a random VIN.
    OEM = vehicle.get_OEM()  # Get the vehicle manufacturer based on VIN.
    model_year = vehicle.get_model_year()  # Get the model year based on the VIN
    time = fake.date_time_between("now", "+1d")  # Generate a random timestamp within the last 30 days.
    speed = random.randint(0,120)  # Generate a random speed in km/h.
    vehicle.update_odometer(random.randint(0, 10))  # Update the odometer with a random increment.
    odometer = vehicle.get_odometer()   # Generate the odometer reading in km.
    geopos = f"{fake.latitude()},{fake.longitude()}"  # Generate random geographic coordinates.
    # Destroy the vehicle object to free memory.
    del vehicle

    # Return all the generated data as a list.
    return [
        VIN, OEM, model_year, time, speed, odometer, geopos
    ]


def _write_to_csv() -> None:
    """
    Generates multiple fake user records and writes them to a CSV file.
    """
    # Create a Faker instance with Mexico data.
    fake = _create_data("es_MX")
    
    # Define the CSV headers.
    headers = [
        "VIN", "OEM","model_year","time", "speed", "odometer", "geopos"
    ]

    # Establish number of rows based on the date.
    if str(date.today()) == "2025-11-27":
        rows = random.randint(1000, 2000)
    else:
        rows = random.randint(50000, 100000)
    
    # Open the CSV file for writing.
    with open("/opt/airflow/data/fleet_raw_data.csv", mode="a", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        #Check if the file is empty to write headers
        if file.tell() == 0:
            writer.writerow(headers)
        
        # Generate and write each record to the CSV.
        for _ in range(rows):
            writer.writerow(_generate_record(fake))
    # Log the action.
    logging.info(f"Written {rows} records to the CSV file.")


def _add_id() -> None:
    """
    Adds a unique UUID to each row in a CSV file.
    """
    
    # Load the CSV into a Polars DataFrame.
    df = pl.read_csv("/opt/airflow/data/fleet_raw_data.csv")
    # Generate a list of UUIDs (one for each row).
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    # Add a new column with unique IDs.
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    # Save the updated DataFrame back to a CSV.
    df.write_csv("/opt/airflow/data/fleet_raw_data.csv")
    # Log the action.
    logging.info("Added UUID to the dataset.")
    


def _update_datetime() -> None:
    """
    Update the 'accessed_at' column in a CSV file with the appropriate timestamp.
    """
        # Change date only for next runs.
    if str(date.today()) != "2024-09-23":
        # Get the current time without milliseconds and calculate yesterday's time.
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        # Load the CSV into a Polars DataFrame.
        df = pl.read_csv("/opt/airflow/data/fleet_raw_data.csv")
        # Replace all values in the 'accessed_at' column with yesterday's timestamp.
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        # Save the updated DataFrame back to a CSV file.
        df.write_csv("/opt/airflow/data/fleet_raw_data.csv")
        # Log the action.
        logging.info("Updated accessed timestamp.")


def save_raw_data():
    '''
    Execute all steps for data generation.
    '''
    # Logging starting of the process.
    logging.info(f"Started batch processing for {date.today()}.")
    # Generate and write records to the CSV.
    _write_to_csv()
    # Add UUID to dataset.
    #_add_id()
    # Update the timestamp.
    #_update_datetime()
    # Logging ending of the process.
    logging.info(f"Finished batch processing {date.today()}.")

#Main

# Define the default arguments for DAG.
default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    }
    
# Define the DAG.
dag = DAG(
    'Vehicle_Fleet_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval="* 7 * * *",
    start_date=datetime(2025, 11, 26),
    catchup=False,
    )

# Define extract raw data task.
extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
    )

# Define create raw schema task.
create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
    )
'''
drop_raw_table_task = SQLExecuteQueryOperator(
    task_id='drop_raw_table',
    conn_id='postgres_conn',
    sql="DROP TABLE IF EXISTS driven_raw.raw_batch_data;",
    dag=dag
)
'''

# Define create raw table task.
create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            VIN VARCHAR(17),
            OEM VARCHAR(20),
            model_year INT,
            time TIMESTAMP,
            speed INT,
            odometer INT,
            geopos VARCHAR(100)
        );
    """,
    dag=dag
)

# Define load CSV data into the table task.
load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
    COPY driven_raw.raw_batch_data(
    VIN, OEM, model_year, time, speed, odometer, geopos
    ) 
    FROM '/opt/airflow/data/fleet_raw_data.csv' 
    DELIMITER ',' 
    CSV HEADER;
    """
)

# Define staging dbt models run task.
run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
)

# Define trusted dbt models run task.
run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
)

#Configure the tasks usgin PythonOperator, SQLExecuteQueryOperator and BashOperator.
# Set the task in the DAG
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task
