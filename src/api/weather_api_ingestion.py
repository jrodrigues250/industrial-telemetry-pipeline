import os
import json
from confluent_kafka import Producer

# Configuração de conexão extraída das variáveis de ambiente / Secrets
kafka_config = {
    'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
}

producer = Producer(kafka_config)
topic_name = 'industrial-weather-data'

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Falha ao entregar mensagem: {err}")
    else:
        print(f"✅ Mensagem enviada para {msg.topic()} [{msg.partition()}] no offset {msg.offset()}")

# Loop onde você obtém os dados de clima
for weather_data in weather_events:
    # Converter para string/bytes JSON
    payload = json.dumps(weather_data).encode('utf-8')
    
    # Enviar ao Confluent Cloud
    producer.produce(
        topic=topic_name,
        key=weather_data['location_city'].encode('utf-8'),
        value=payload,
        callback=delivery_report
    )

# Forçar a entrega de todas as mensagens pendentes antes de encerrar
producer.flush()
