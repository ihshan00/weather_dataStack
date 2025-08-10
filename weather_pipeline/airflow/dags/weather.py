from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    schedule_interval='*/1 * * * *',   
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    ingest = PythonOperator(
        task_id='fetch_and_publish',
        python_callable=lambda: __import__('../ingestion.ingest').publish()
    )
