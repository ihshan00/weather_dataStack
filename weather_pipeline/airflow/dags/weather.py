from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from ingestion.ingest import publish
default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

"""
In Airflow Admin → Connections:

Create a new connection with:

    Conn Id: spark_default

    Conn Type: Spark

    Master URL: spark://spark-master:7077

    Deploy mode: client
"""


with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    schedule_interval='*/1 * * * *',   
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    ingest = PythonOperator(
        task_id='fetch_and_publish',
        python_callable=publish
    )
    consume_kafka_task = SparkSubmitOperator(
        task_id="consume_kafka_task",
        application="/opt/airflow/dags/consume_kafka.py",  
        conn_id="spark_default",  
        verbose=True,
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.2,"
            "io.delta:delta-core_2.13:2.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ),
        name="ConsumeKafkaToDelta",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        },
    )
    ingest >> consume_kafka_task
