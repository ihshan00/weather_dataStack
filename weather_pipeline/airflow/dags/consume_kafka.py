import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Configure logging
logging.basicConfig(level=logging.INFO)


def create_spark_connection():
    """
    Create and return a SparkSession configured for Kafka, Delta Lake, and MinIO.
    """
    try:
        spark = SparkSession.builder \
            .appName('WeatherKafkaToDelta') \
            .config('spark.jars.packages', \
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,io.delta:delta-core_2.13:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4') \
            .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
            .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
            .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .config('spark.delta.logStore.class', \
                    'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore') \
            .getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        logging.info('SparkSession created successfully.')
        return spark
    except Exception as e:
        logging.error(f'Error creating SparkSession: {e}')
        raise


def connect_to_kafka(spark, topic='weather_raw', bootstrap_servers='broker:9092'):
    """
    Create a streaming DataFrame by reading from Kafka.
    """
    try:
        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', bootstrap_servers) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'latest') \
            .load()
        logging.info(f'Connected to Kafka topic: {topic}')
        return df
    except Exception as e:
        logging.error(f'Error connecting to Kafka: {e}')
        raise


def create_selection_df_from_kafka(kafka_df):
    """
    Parse Kafka JSON messages into a structured DataFrame.
    """
    schema = StructType([  
        StructField('city', StringType(), False),
        StructField('temperature', DoubleType(), False),
        StructField('windspeed', DoubleType(), False),
        StructField('time', StringType(), False),
        StructField('ts', StringType(), False)
    ])
    try:
        parsed = kafka_df.selectExpr('CAST(value AS STRING) as json_str') \
            .select(from_json(col('json_str'), schema).alias('data')) \
            .select('data.*')
        logging.info('Parsed Kafka stream into DataFrame.')
        return parsed
    except Exception as e:
        logging.error(f'Error parsing Kafka messages: {e}')
        raise


def write_stream_to_delta(df, output_path='s3a://weather-bucket/processed/', checkpoint_path='s3a://weather-bucket/checkpoints/'):
    """
    Write the streaming DataFrame to Delta Lake on MinIO.
    """
    try:
        query = df.writeStream \
            .format('delta') \
            .outputMode('append') \
            .option('path', output_path) \
            .option('checkpointLocation', checkpoint_path) \
            .start()
        logging.info(f'Streaming write to Delta started: {output_path}')
        return query
    except Exception as e:
        logging.error(f'Error starting Delta write stream: {e}')
        raise


if __name__ == '__main__':
    # Initialize Spark
    spark = create_spark_connection()

    # Connect to Kafka
    kafka_df = connect_to_kafka(spark)

    # Parse messages
    weather_df = create_selection_df_from_kafka(kafka_df)

    # Write to Delta Lake on MinIO
    query = write_stream_to_delta(weather_df)

    # Await termination
    logging.info('Streaming query running. Awaiting termination...')
    query.awaitTermination()
