from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajuste de sys.path para encontrar el paquete 'etl'
current_dir = Path(__file__).parent
project_root = current_dir.parent  # /opt/airflow/etl
sys.path.insert(0, str(project_root))

# Importar funciones desde el paquete etl
from etl.download_data import check_page_status, download_csv
from etl.data_reader import load_csv_data
from etl.data_cleaner import clean_data
from etl.schema_creator import create_all_tables
from etl.load_data import (
    load_dim_vehicle,
    load_dim_date,
    load_dim_location,
    load_dim_electric_type,
    load_dim_policy,
    load_fact_registration,
)

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'electric_vehicles_etl',
    default_args=default_args,
    description='ETL para datos de vehículos eléctricos',
    schedule_interval=timedelta(days=7),
    catchup=False,
    max_active_runs=1,
)

# Parámetros
URL = "https://catalog.data.gov/dataset/electric-vehicle-population-data"
DATA_DIR = '/opt/airflow/data'
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = os.path.join(DATA_DIR, 'cleaned_data.parquet')

# ---------- Funciones de tareas

def check_source():
    return check_page_status(URL)


def download_data():
    download_csv(URL, DATA_DIR)
    # Se asume que download_csv guarda como electric_vehicles_1.csv
    return os.path.join(DATA_DIR, 'electric_vehicles_1.csv')


def process_data(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='download_data')
    df = load_csv_data(csv_path)
    if df is not None:
        clean_data(df, OUTPUT_FILE)
    else:
        raise ValueError("No se pudo cargar el CSV")


def create_db_tables():
    create_all_tables()

# ---------- Operadores Airflow
check_source_task = PythonOperator(
    task_id='check_source_availability',
    python_callable=check_source,
    dag=dag,
)

download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

clean_transform_task = PythonOperator(
    task_id='clean_transform_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id='create_database_tables',
    python_callable=create_db_tables,
    dag=dag,
)

load_dim_vehicle_task = PythonOperator(
    task_id='load_dim_vehicle',
    python_callable=load_dim_vehicle,
    dag=dag,
)

load_dim_date_task = PythonOperator(
    task_id='load_dim_date',
    python_callable=load_dim_date,
    dag=dag,
)

load_dim_location_task = PythonOperator(
    task_id='load_dim_location',
    python_callable=load_dim_location,
    dag=dag,
)

load_dim_electric_type_task = PythonOperator(
    task_id='load_dim_electric_type',
    python_callable=load_dim_electric_type,
    dag=dag,
)

load_dim_policy_task = PythonOperator(
    task_id='load_dim_policy',
    python_callable=load_dim_policy,
    dag=dag,
)

load_fact_task = PythonOperator(
    task_id='load_fact_table',
    python_callable=load_fact_registration,
    dag=dag,
)

# ---------- Definición del flujo
check_source_task >> download_data_task >> clean_transform_task >> create_tables_task

create_tables_task >> [
    load_dim_vehicle_task,
    load_dim_date_task,
    load_dim_location_task,
    load_dim_electric_type_task,
    load_dim_policy_task,
] >> load_fact_task
