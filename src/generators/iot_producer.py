import os
import json
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configuração de conexão com o Confluent Cloud
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

# Callback para relatório de entrega das mensagens
def delivery_report(err, msg):
    if err is not None:
        print(f"[-] Erro ao entregar mensagem: {err}")
    else:
        print(f"[+] Evento enviado -> Tópico: {msg.topic()} | Partição: {msg.partition()} | Offset: {msg.offset()}")

producer = Producer(conf)
topic_name = 'industrial-iot-telemetry'

# Mapeamento de equipamentos para simulação industrial
EQUIPMENTS = [
    {"sensor_id": "sensor_press_01", "location": "sector_a_boiler", "type": "pressure"},
    {"sensor_id": "sensor_temp_01", "location": "sector_a_boiler", "type": "temperature"},
    {"sensor_id": "sensor_vibr_01", "location": "sector_b_turbine", "type": "vibration"},
    {"sensor_id": "sensor_temp_02", "location": "sector_b_turbine", "type": "temperature"},
]

def generate_telemetry_event(equipment):
    """Gera dados simulados com variações realistas para telemetria industrial."""
    status_weights = ["OPERATIONAL", "WARNING", "CRITICAL"]
    # 90% operacional, 8% aviso, 2% crítico
    status = random.choices(status_weights, weights=[0.90, 0.08, 0.02])[0]
    
    val = 0.0
    if equipment["type"] == "temperature":
        val = round(random.uniform(65.0, 95.0), 2)
    elif equipment["type"] == "pressure":
        val = round(random.uniform(10.0, 35.0), 2)
    elif equipment["type"] == "vibration":
        val = round(random.uniform(0.1, 5.5), 2)

    return {
        "sensor_id": equipment["sensor_id"],
        "location": equipment["location"],
        "metric_type": equipment["type"],
        "value": val,
        "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat()
    }

print(f"[*] Iniciando streaming continuo de telemetria IoT para o topico '{topic_name}'...")
print("[*] Pressione Ctrl + C para encerrar a execucao.\n")

try:
    while True:
        # Seleciona um equipamento aleatório a cada iteração
        eq = random.choice(EQUIPMENTS)
        payload = generate_telemetry_event(eq)
        
        producer.produce(
            topic=topic_name,
            key=payload["sensor_id"],
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        
        # Atende os callbacks de envio pendentes
        producer.poll(0)
        
        time.sleep(2)  # Aguarda 2 segundos entre os eventos

except KeyboardInterrupt:
    print("\n[*] Encerrando o produtor de telemetria...")
finally:
    print("[*] Esvaziando a fila de mensagens pendentes...")
    producer.flush()
    print("[+] Produtor finalizado com sucesso.")