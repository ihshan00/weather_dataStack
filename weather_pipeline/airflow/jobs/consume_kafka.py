from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
from datetime import datetime
import os

# Kafka and S3 configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
S3_OUTPUT_PATH = os.getenv("S3_OUTPUT_PATH", "s3a://weather-data/output/")

# Define schema for Kafka messages
weather_schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

def get_spark_session():
    spark = SparkSession.builder \
        .appName("ConsumeKafkaToMinIO") \
        .getOrCreate()
    return spark

def read_from_kafka(spark):
    """Read Kafka messages once as a batch."""
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert Kafka value from bytes to string and parse JSON
    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), weather_schema).alias("data")) \
        .select("data.*")
    return df_parsed

def write_to_minio(df):
    """Write DataFrame once to Parquet with a timestamped filename."""
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(S3_OUTPUT_PATH, f"weather_{timestamp_str}.parquet")

    df.write \
      .mode("overwrite") \
      .parquet(output_path)

    print(f"Data written successfully to: {output_path}")

if __name__ == "__main__":
    spark = get_spark_session()
    kafka_df = read_from_kafka(spark)
    write_to_minio(kafka_df)
    spark.stop()

