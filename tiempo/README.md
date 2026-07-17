# Caso 3 — De AEMET a MinIO y MongoDB con Kafka (arquitectura Medallion)

Pipeline que recoge datos meteorológicos de AEMET (vía el-tiempo.net) y los mueve
por tres capas de calidad creciente (bronze, silver, gold) usando Kafka como
elemento de integración entre productores y consumidores.

## Arquitectura

API el-tiempo.net -> Productor -> topic bronze -> Consumidor bronze -> MinIO (bronze/)
                                -> topic silver -> Consumidor silver -> MinIO (silver/) + MongoDB
                                                                      -> cada 10 mensajes -> topic gold (media temp/humedad)

## Componentes

- src/config.py: configuración compartida, leída de .env
- src/productor.py: consulta la API cada minuto, publica en bronze y silver
- src/consumidor_bronze.py: guarda los mensajes de bronze en MinIO
- src/consumidor_silver.py: guarda los mensajes de silver en MinIO y MongoDB,
  y cada 10 mensajes calcula la media de temperatura/humedad y la publica en gold

## Infraestructura (docker-compose.yml)

- kafka: broker único, topics bronze / silver / gold
- minio: almacenamiento tipo S3, bucket "tiempo"
- mongo: base de datos "tiempo", colección "datos"

## Puesta en marcha

1. Levantar infraestructura:
   docker compose up -d

2. Crear los topics dentro del contenedor de Kafka:
   docker exec -it --workdir /opt/kafka/bin iabd-kafka bash
   ./kafka-topics.sh --create --topic bronze --bootstrap-server kafka:9092
   ./kafka-topics.sh --create --topic silver --bootstrap-server kafka:9092
   ./kafka-topics.sh --create --topic gold --bootstrap-server kafka:9092

3. Instalar dependencias:
   pip install --break-system-packages confluent-kafka requests boto3 pymongo pandas python-dotenv

4. Configurar .env (bootstrap servers, credenciales MinIO, MONGO_URI, etc.)

5. Ejecutar los tres scripts, cada uno en su propia terminal:
   python3 src/productor.py
   python3 src/consumidor_bronze.py
   python3 src/consumidor_silver.py

## Verificación

Ver mensajes en el topic gold:
   docker exec -it --workdir /opt/kafka/bin iabd-kafka ./kafka-console-consumer.sh --topic gold --from-beginning --bootstrap-server kafka:9092

Consola MinIO: http://localhost:9001 (minioadmin / minioadmin123), bucket "tiempo"
