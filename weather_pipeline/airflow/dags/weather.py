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
    schedule_interval='*/5 * * * *',   # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    ingest = PythonOperator(
        task_id='fetch_and_publish',
        python_callable=lambda: __import__('ingestion.ingest').publish(),
    )

    # spark_raw = SparkSubmitOperator(
    #     task_id='spark_raw_etl',
    #     application='/opt/spark/jobs/submit_raw.py',
    #     conn_id='spark_default',
    # )

    # spark_agg = SparkSubmitOperator(
    #     task_id='spark_aggregate',
    #     application='/opt/spark/jobs/compute_aggregates.py',
    #     conn_id='spark_default',
    # )

    # dbt_run = BashOperator(
    #     task_id='dbt_transform',
    #     bash_command='cd /opt/dbt && dbt run && dbt test',
    # )

    # load_postgres = PostgresOperator(
    #     task_id='load_to_postgres',
    #     postgres_conn_id='postgres_default',
    #     sql='sql/load_hourly_weather.sql',
    # )

    # Set dependencies
    # ingest >> spark_raw >> spark_agg >> dbt_run >> load_postgres
    ingest