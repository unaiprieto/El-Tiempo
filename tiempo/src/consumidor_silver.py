from datetime import datetime
from json import loads, dumps

import pandas as pd
from confluent_kafka import Consumer, Producer, KafkaError
from pymongo import MongoClient

import config


def esquema_json(dct):
    result = {}
    if "fecha" in dct:
        result["fecha"] = datetime.fromisoformat(dct["fecha"])
    if "temp" in dct:
        result["temp"] = float(dct["temp"])
    if "humedad" in dct:
        result["humedad"] = float(dct["humedad"])
    if "ciudad" in dct:
        result["ciudad"] = dct["ciudad"]
    return result


def delivery_report(err, msg):
    if err is not None:
        print(f"Error entregando a {msg.topic()}: {err}")
    else:
        print(f"Mensaje gold enviado [P:{msg.partition()} O:{msg.offset()}]")


def calcular_agregado(mensajes):
    pd_mensajes = pd.DataFrame(mensajes)
    pd_agg = (
        pd_mensajes.groupby("ciudad")
        .agg(fecha=("fecha", "max"), temp=("temp", "mean"), humedad=("humedad", "mean"))
        .reset_index()
    )
    registros = pd_agg.to_dict(orient="records")
    for r in registros:
        if hasattr(r["fecha"], "isoformat"):
            r["fecha"] = r["fecha"].isoformat()
    return dumps(registros).encode("utf-8")


def main():
    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": "grupo-silver",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.TOPIC_SILVER])

    bucket = config.s3_resource().Bucket(config.S3_BUCKET)

    cliente_mongo = MongoClient(config.MONGO_URI)
    coleccion = cliente_mongo[config.MONGO_DB][config.MONGO_COLLECTION]

    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "client.id": "productor-gold",
        "acks": "all",
        "enable.idempotence": True,
    })

    print(f"Consumidor silver / productor gold iniciado. Batch size: {config.GOLD_BATCH_SIZE}")
    mensajes = []

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

            valor_bytes = m.value()
            try:
                doc_json = loads(valor_bytes.decode("utf-8"), object_hook=esquema_json)
            except (ValueError, KeyError) as e:
                print(f"Mensaje con formato inesperado, se descarta: {e}")
                continue

            mensajes.append(doc_json)

            nom_fichero = "silver/" + datetime.now().isoformat() + ".json"
            bucket.put_object(Key=nom_fichero, Body=valor_bytes)

            coleccion.insert_one(doc_json.copy())

            if len(mensajes) >= config.GOLD_BATCH_SIZE:
                mensaje_gold = calcular_agregado(mensajes)
                print(f"Mensaje gold: {mensaje_gold.decode('utf-8')}")
                producer.produce(config.TOPIC_GOLD, value=mensaje_gold, callback=delivery_report)
                producer.poll(0)
                mensajes = []
    except KeyboardInterrupt:
        print("Deteniendo consumidor silver / productor gold...")
    finally:
        producer.flush(timeout=10)
        consumer.close()
        cliente_mongo.close()


if __name__ == "__main__":
    main()
