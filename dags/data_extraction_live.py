from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import os
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stream.nasa_neo_data import nasa_neo_data_api,produce_nasa_data,nasa_neo_consumer_data,nasa_neo_data_transformation,upload_to_s3


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'asteroid_real_time_pipeline',
    default_args=default_args,
    description='A Real time pipeline for asteroid data processing',
    schedule_interval=timedelta(days=1),
)


read_nasa_neo_data = PythonOperator(
    task_id='read_data_nasa_neo',
    provide_context=True,
    python_callable=nasa_neo_data_api,
    dag=dag
)

prodcue_data = PythonOperator(
    task_id='produce_data',
    provide_context=True,
    python_callable=produce_nasa_data,
    dag=dag
)

consumer_data = PythonOperator(
    task_id='consumer_data',
    provide_context=True,
    python_callable=nasa_neo_consumer_data,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=nasa_neo_data_transformation,
    dag=dag
)


upload_data_to_s3 = PythonOperator(
    task_id='upload_data_to_s3',
    provide_context=True,
    python_callable=upload_to_s3,
    dag=dag
)


read_nasa_neo_data >> prodcue_data >> consumer_data >> transform_data >> upload_data_to_s3
