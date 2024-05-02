from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_lake_to_data_warehouse', default_args=default_args, schedule_interval='@daily')

def load_data_to_data_warehouse():
    # Code to load data from data lake to data warehouse
    pass

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_data_warehouse,
    dag=dag
)