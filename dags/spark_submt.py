from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_aggregation.close_approach_transformation import close_approach_analysis
from data_aggregation.cluster_based_analysis import cluster_analysis
from data_aggregation.kpi import kpi_data

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag =  DAG(
    'spark_submit',
    default_args=default_args,
    description='A spark submt dag',
    schedule_interval=timedelta(days=1)
) 

# Task to run the Spark start script
start_spark_script = BashOperator(
    task_id='start_spark_script',
    bash_command='/home/ubuntu/airflow/scripts/start_spark.sh >> /home/ubuntu/airflow/scripts/start_spark.log 2>&1',
    dag=dag,
)


union_hist_live_data = SparkSubmitOperator(
    task_id='union_hist_live_data',
    conn_id='spark_default',
    application='/home/ubuntu/airflow/data_aggregation/union_hist_live_data.py',
    executor_memory='5g',
    total_executor_cores=2,
    conf={
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563"
    },
    dag=dag
)

close_apparoach_analysis =  PythonOperator(
    task_id="close_apparoach_task",
    python_callable=close_approach_analysis,
    dag=dag
)


cluster_analysis_task = PythonOperator(
    task_id="cluster_analysis",
    python_callable=cluster_analysis,
    dag=dag
)

kpi_analysis_task = PythonOperator(
    task_id="kpi_analysis",
    python_callable=kpi_data,
    dag=dag
)

start_spark_script >> union_hist_live_data >> [close_apparoach_analysis,cluster_analysis_task,kpi_analysis_task]
