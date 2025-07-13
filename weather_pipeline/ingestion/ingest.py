import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables from .env
load_dotenv()

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")

# Open-Meteo city coordinates
CITIES = {
    "Colombo": {"latitude": 6.9271, "longitude": 79.8612},
    "Kandy":   {"latitude": 7.2906, "longitude": 80.6337},
    "Galle":   {"latitude": 6.0535, "longitude": 80.2210}
}

# Data parameters
HOURLY_PARAMS = ["temperature_2m", "precipitation"]
TIMEZONE = os.getenv("TIMEZONE", "Asia/Colombo")
INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL", 300))  # default 5 minutes


def  fetch_open_meteo(
    city: str = "Colombo",
    coords: dict = {"latitude": 6.9271, "longitude": 79.8612},
    start: str = "2025-05-02",
    end: str = "2025-05-03"
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
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

    try:
        while True:
            # Define date range for current fetch
            today = time.strftime("%Y-%m-%d", time.localtime())
            # fetch next day as end_date to cover an hour window
            tomorrow = time.strftime("%Y-%m-%d", time.localtime(time.time() + 86400))

            for city, coords in CITIES.items():
                payload = fetch_open_meteo(city, coords, today, tomorrow)
                producer.send(TOPIC, value=payload)
                print(f"Sent data for {city} at {payload['fetched_at']}")

            producer.flush()
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Stopping ingestion...")
    finally:
        producer.close()

publish()

