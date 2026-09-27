import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer


def load_assets(csv_path="data/assets_cmms.csv"):
    assets = []
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                assets.append(row["asset_id"])
    except FileNotFoundError:
        print(
            f"Erro: Arquivo {csv_path} não encontrado. Usando ativos padrão."
        )
        assets = [f"AST-10{i}" for i in range(1, 11)]
    return assets


def generate_telemetry_event(asset_id):
    # Simula operação normal vs. anomalias aleatórias (10% de chance de anomalia)
    is_anomaly = random.random() < 0.10

    if is_anomaly:
        temperature = round(random.uniform(85.0, 110.0), 2)  # Superaquecimento
        vibration = round(random.uniform(7.5, 12.0), 2)  # Vibração crítica
        status = "WARNING" if temperature < 100 else "CRITICAL"
    else:
        temperature = round(random.uniform(45.0, 75.0), 2)  # Operação normal
        vibration = round(random.uniform(0.5, 4.5), 2)  # Vibração normal
        status = "OPERATIONAL"

    pressure = round(random.uniform(2.0, 6.0), 2)

    payload = {
        "event_id": f"evt_{int(time.time()*1000)}_{asset_id}",
        "asset_id": asset_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "temperature_celsius": temperature,
            "vibration_mm_s": vibration,
            "pressure_bar": pressure,
        },
        "operating_status": status,
    }
    return payload


def delivery_report(err, msg):
    """Callback chamado para confirmar a entrega da mensagem no Kafka."""
    if err is not None:
        print(f"❌ Falha ao entregar mensagem: {err}")
    else:
        print(
            f"✅ Enviado -> Tópico: {msg.topic()} | Partição: [{msg.partition()}] | Offset: {msg.offset()}"
        )


def run_generator():
    assets = load_assets()
    print(f"--- Iniciando Simulador IoT para {len(assets)} ativos ---")

    # Configuração do Confluent Cloud via variáveis de ambiente
    bootstrap_servers = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
    api_key = os.getenv("CONFLUENT_API_KEY")
    api_secret = os.getenv("CONFLUENT_API_SECRET")
    topic = "industrial-iot-telemetry"

    # Se as credenciais estiverem presentes, habilita a gravação no Kafka
    producer = None
    if bootstrap_servers and api_key and api_secret:
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
        }
        producer = Producer(conf)
        print("⚡ Conexão com Confluent Cloud configurada.")
    else:
        print("⚠️ Variáveis de ambiente do Confluent não encontradas. Executando em modo apenas local (print).")

    events = []
    for asset_id in assets:
        event = generate_telemetry_event(asset_id)
        events.append(event)
        
        payload_str = json.dumps(event)
        print(payload_str)

        # Envia para o Confluent Cloud se o producer estiver ativo
        if producer:
            producer.produce(
                topic=topic,
                key=asset_id,
                value=payload_str,
                callback=delivery_report,
            )
            # Servir eventos de callback pendentes
            producer.poll(0)

    # Forçar o envio de todas as mensagens bufferizadas no Kafka
    if producer:
        print("⏳ Despachando mensagens pendentes para o broker...")
        producer.flush()
        print("🎉 Envio concluído com sucesso!")

    return events


if __name__ == "__main__":
    run_generator()
