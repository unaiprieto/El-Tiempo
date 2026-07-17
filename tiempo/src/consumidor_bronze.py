from datetime import datetime

from confluent_kafka import Consumer, KafkaError

import config


def crear_consumidor():
    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": "grupo-bronze",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.TOPIC_BRONZE])
    return consumer


def main():
    consumer = crear_consumidor()
    bucket = config.s3_resource().Bucket(config.S3_BUCKET)

    print(f"Consumidor bronze iniciado. Topic: {config.TOPIC_BRONZE}")

    try:
        while True:
            m = consumer.poll(1.0)
            if m is None:
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Error: {m.error()}")
                continue

            nom_fichero = "bronze/" + datetime.now().isoformat() + ".json"
            bucket.put_object(Key=nom_fichero, Body=m.value())
            print(f"Guardado {nom_fichero} (P:{m.partition()} O:{m.offset()})")
    except KeyboardInterrupt:
        print("Deteniendo consumidor bronze...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
