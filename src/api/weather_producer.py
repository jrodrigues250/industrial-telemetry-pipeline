import os
import json
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

def delivery_report(err, msg):
    if err is not None:
        print(f"[-] Erro ao entregar mensagem de clima: {err}")
    else:
        print(f"[+] Clima enviado -> Tópico: {msg.topic()} | Partição: {msg.partition()} | Offset: {msg.offset()}")

producer = Producer(conf)
topic_name = 'industrial-weather-data'

PLANTS = [
    {"plant_id": "plant_sp_01", "city": "Sao Paulo", "lat": -23.5505, "lon": -46.6333},
    {"plant_id": "plant_rj_01", "city": "Rio de Janeiro", "lat": -22.9068, "lon": -43.1729}
]

def generate_weather_event(plant):
    return {
        "plant_id": plant["plant_id"],
        "city": plant["city"],
        "ambient_temperature": round(random.uniform(18.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 90.0), 2),
        "wind_speed_kmh": round(random.uniform(5.0, 25.0), 2),
        "condition": random.choice(["CLEAR", "CLOUDY", "RAIN", "THUNDERSTORM"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

print(f"[*] Iniciando produtor de clima para o tópico '{topic_name}'...")
print("[*] Pressione Ctrl + C para encerrar.\n")

try:
    while True:
        plant = random.choice(PLANTS)
        payload = generate_weather_event(plant)
        
        producer.produce(
            topic=topic_name,
            key=payload["plant_id"],
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        
        producer.poll(0)
        time.sleep(5)  # Envia atualizações a cada 5 segundos

except KeyboardInterrupt:
    print("\n[*] Encerrando produtor de clima...")
finally:
    producer.flush()
    print("[+] Produtor de clima finalizado.")