import time
from datetime import datetime
from json import dumps

import requests
from confluent_kafka import Producer

import config


def delivery_report(err, msg):
    if err is not None:
        print(f"Error entregando mensaje: {err}")
    else:
        print(f"Entregado a {msg.topic()} [P:{msg.partition()} O:{msg.offset()}]")


def crear_productor():
    return Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "client.id": "productor-tiempo",
        "acks": "all",
        "enable.idempotence": True,
    })


def consultar_aemet():
    try:
        r = requests.get(config.AEMET_URL, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error consultando AEMET: {e}")
        return None


def construir_mensaje_silver(resp_json):
    return {
        "fecha": datetime.now().isoformat(),
        "ciudad": resp_json["municipio"]["NOMBRE"],
        "temp": resp_json["temperatura_actual"],
        "humedad": resp_json["humedad"],
    }


def main():
    producer = crear_productor()
    print(f"Productor iniciado. Bootstrap: {config.BOOTSTRAP_SERVERS}")

    try:
        while True:
            resp_json = consultar_aemet()
            if resp_json is None:
                time.sleep(10)
                continue

            producer.produce(
                config.TOPIC_BRONZE,
                value=dumps(resp_json).encode("utf-8"),
                callback=delivery_report,
            )

            try:
                datos_json = construir_mensaje_silver(resp_json)
            except (KeyError, TypeError) as e:
                print(f"Respuesta con formato inesperado, se omite silver: {e}")
                producer.poll(0)
                time.sleep(config.POLL_INTERVAL_SECONDS)
                continue

            producer.produce(
                config.TOPIC_SILVER,
                value=dumps(datos_json).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(config.POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Deteniendo productor...")
    finally:
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
