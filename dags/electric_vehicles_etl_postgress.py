from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Importar funciones desde los paquetes etl
from etl.download_data import check_page_status, download_csv
from etl.data_cleaner import clean_data
from etl.load_data import (
    load_dim_vehicle, load_dim_date, load_dim_location,
    load_dim_electric_type, load_dim_policy, load_fact_registration
)

# Configuración del DAG
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Parámetros
URL = "https://catalog.data.gov/dataset/electric-vehicle-population-data"
DATA_DIR = '/opt/airflow/data'
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = os.path.join(DATA_DIR, 'cleaned_data.parquet')
SQL_FILE = 'sql/tables.sql'

with DAG(
    'electric_vehicles_etl_v2',
    default_args=default_args,
    description='ETL completo para datos de vehículos eléctricos',
    schedule_interval='0 * * * *',
    start_date=datetime(2025,5,25),
    catchup=False,
    max_active_runs=1
) as dag:
    
    # Verificar disponibilidad del origen
    check_source_task = PythonOperator(
        task_id='check_source_availability',
        python_callable=check_page_status,
        op_kwargs={"url": URL}
    )

    # Descargar datos CSV
    download_csv_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
        op_kwargs={
            "url": URL,
            "output_dir": DATA_DIR
        }
    )

    # Crear tablas en la base de datos
    create_tables_task = PostgresOperator(
        task_id='create_database_tables',
        postgres_conn_id="postgres_promtior_etl_db",
        sql=SQL_FILE, 
    )

    # Limpiar y procesar datos
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        op_kwargs={
            "input_file": "{{ ti.xcom_pull(task_ids='download_csv') }}",
            "output_file": OUTPUT_FILE
        }
    )

    # Cargar dimensiones
    load_dim_vehicle_task = PythonOperator(
        task_id='load_dim_vehicle',
        python_callable=load_dim_vehicle,
        op_kwargs={
            "data_file_path": "{{ ti.xcom_pull(task_ids='clean_data') }}"
        }
    )

    load_dim_date_task = PythonOperator(
        task_id='load_dim_date',
        python_callable=load_dim_date,
        op_kwargs={
            "data_file_path": "{{ ti.xcom_pull(task_ids='clean_data') }}"
        }
    )

    load_dim_location_task = PythonOperator(
        task_id='load_dim_location',
        python_callable=load_dim_location,
        op_kwargs={
            "data_file_path": "{{ ti.xcom_pull(task_ids='clean_data') }}"
        }
    )

    load_dim_electric_type_task = PythonOperator(
        task_id='load_dim_electric_type',
        python_callable=load_dim_electric_type,
        op_kwargs={
            "data_file_path": "{{ ti.xcom_pull(task_ids='clean_data') }}"
        }
    )

    load_dim_policy_task = PythonOperator(
        task_id='load_dim_policy',
        python_callable=load_dim_policy,
        op_kwargs={
            "data_file_path": "{{ ti.xcom_pull(task_ids='clean_data') }}"
        }
    )

    # Cargar tabla de hechos
    load_fact_registration_task = PythonOperator(
        task_id='load_fact_registration',
        python_callable=load_fact_registration,
        op_kwargs={
            "data_file_path": "{{ ti.xcom_pull(task_ids='clean_data') }}"
        }
    )

    # Definición del flujo
    check_source_task >> download_csv_task >> create_tables_task >> clean_data_task
    
    # Las dimensiones se pueden cargar en paralelo
    clean_data_task >> [
        load_dim_vehicle_task,
        load_dim_date_task,
        load_dim_location_task, 
        load_dim_electric_type_task,
        load_dim_policy_task
    ] >> load_fact_registration_task
    