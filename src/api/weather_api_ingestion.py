import csv
import json
import os
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
import requests


def load_asset_locations(csv_path="data/assets_cmms.csv"):
    locations = {}
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                city = row["location_city"]
                if city not in locations:
                    locations[city] = {
                        "latitude": float(row["latitude"]),
                        "longitude": float(row["longitude"]),
                        "state": row["location_state"],
                    }
    except FileNotFoundError:
        print(f"Erro: Arquivo {csv_path} não encontrado.")
    return locations


def fetch_weather_data(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
        ],
        "timezone": "UTC",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        current = data.get("current", {})

        return {
            "ambient_temperature_celsius": current.get("temperature_2m"),
            "relative_humidity_pct": current.get("relative_humidity_2m"),
            "surface_pressure_hpa": current.get("surface_pressure"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
        }
    except requests.RequestException as e:
        print(f"Erro na chamada da API Open-Meteo: {e}")
        return None


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Falha ao entregar mensagem no Kafka: {err}")
    else:
        print(
            f"✅ Mensagem enviada para {msg.topic()} [{msg.partition()}] no offset {msg.offset()}"
        )


def run_weather_ingestion():
    locations = load_asset_locations()
    print(f"--- Coletando dados climaticos para {len(locations)} cidades ---")

    # Configuração do Producer usando as variáveis de ambiente/Secrets do GitHub
    kafka_config = {
        "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("CONFLUENT_API_KEY"),
        "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
    }

    producer = Producer(kafka_config)
    topic_name = "industrial-weather-data"

    weather_records = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for city, geo in locations.items():
        metrics = fetch_weather_data(geo["latitude"], geo["longitude"])

        if metrics:
            record = {
                "weather_event_id": f"wtr_{int(time.time())}_{city.lower().replace(' ', '_')}",
                "timestamp": timestamp,
                "location_city": city,
                "location_state": geo["state"],
                "latitude": geo["latitude"],
                "longitude": geo["longitude"],
                "metrics": metrics,
            }
            weather_records.append(record)

            # Publicação no tópico do Confluent Kafka
            payload = json.dumps(record).encode("utf-8")
            producer.produce(
                topic=topic_name,
                key=city.encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )

    # Garante a entrega de todas as mensagens antes de encerrar a execução
    producer.flush()
    return weather_records


if __name__ == "__main__":
    run_weather_ingestion()
