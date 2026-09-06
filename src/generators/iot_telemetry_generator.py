import csv
import json
import random
import time
from datetime import datetime, timezone


def load_assets(csv_path="data/assets_cmms.csv"):
    assets = []
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                assets.append(row["asset_id"])
    except FileNotFoundError:
        print(
            f"Erro: Arquivo {csv_path} não encontrado. Usando ativos padrao."
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


def run_generator():
    assets = load_assets()
    print(f"--- Iniciando Simulador IoT para {len(assets)} ativos ---")

    events = []
    for asset_id in assets:
        event = generate_telemetry_event(asset_id)
        events.append(event)
        print(json.dumps(event, indent=2))

    return events


if __name__ == "__main__":
    run_generator()
