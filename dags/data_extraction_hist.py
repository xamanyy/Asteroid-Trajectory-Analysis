from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from batch.asteroid_data_extraction_api import nasa_neo_data_api


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'asteroid_batch_pipeline',
    default_args=default_args,
    description='A batch pipeline for asteroid data processing',
    schedule_interval=timedelta(days=1),
)

#Calling API to fetch data
fetch_nasa_neo_data = PythonOperator(
    task_id='fetch_data_nasa_neo',
    python_callable=nasa_neo_data_api,
    dag=dag
)


fetch_nasa_neo_data