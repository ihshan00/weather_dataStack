import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
import pendulum
import logging
# Load environment variables from .env
load_dotenv()
import sys
from datetime import datetime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'broker:29092')
TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")

# Open-Meteo city coordinates
CITIES = {
    "Colombo": {"latitude": 6.9271, "longitude": 79.8612}
}

# Data parameters
HOURLY_PARAMS = ["temperature_2m", "precipitation"]
TIMEZONE = os.getenv("TIMEZONE", "Asia/Colombo")
INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL", 300))  # default 5 minutes
now = pendulum.now("UTC")

# Yesterday and today
yesterday = now.subtract(days=1).to_date_string()  
today = now.to_date_string()  

def  fetch_open_meteo(
    city: str = "Colombo",
    coords: dict = {"latitude": 6.9271, "longitude": 79.8612},
    start: str = yesterday,
    end: str = today
    ) -> dict:
    """
    Fetch hourly weather data from Open-Meteo for a given city.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "hourly": ",".join(HOURLY_PARAMS),
        "start_date": start,
        "end_date": end,
        "timezone": TIMEZONE
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    # Attach metadata
    return {
        "city": city,
        "fetched_at": time.time(),
        "parameters": HOURLY_PARAMS,
        "data": data
    }


def publish():
    logging.info("Initiating the Producer")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )
    logging.info(f"Producer initiated to {BOOTSTRAP_SERVERS}")

    try:
        for city, coords in CITIES.items():
            payload = fetch_open_meteo(city, coords, yesterday, today)
            producer.send(TOPIC, value=payload)

            # Convert timestamp to human-readable format
            dt = datetime.fromtimestamp(payload["fetched_at"])
            formatted = dt.strftime("%Y-%m-%d %H:%M:%S")

            logging.info(f"Sent data for {city} at {formatted}")

        producer.flush()
        logging.info("Job finished successfully")

    except Exception as e:
        logging.error(f"Exception caused: {e}")
        raise  # important for Airflow to catch failures

    finally:
        producer.close()

