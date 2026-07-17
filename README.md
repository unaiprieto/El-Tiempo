# Caso 3 — De AEMET a MinIO y MongoDB con Kafka (arquitectura Medallion)

Pipeline de streaming que recoge datos meteorológicos de AEMET (a través de la
API de [el-tiempo.net](https://www.el-tiempo.net/api)) y los mueve por tres
capas de calidad creciente — **bronze**, **silver** y **gold** — utilizando
**Apache Kafka** como capa de integración, de forma que el productor y los
consumidores quedan completamente desacoplados entre sí.

## Arquitectura

```
                              Kafka topics
                       ┌─────────────────────┐
API el-tiempo.net      │      bronze         │──▶ Consumidor bronze ──▶ MinIO (bronze/)
   (GET cada 60s)      │                     │
        │              │      silver         │──▶ Consumidor silver ──▶ MinIO (silver/)
        ▼              │                     │                       └─▶ MongoDB (tiempo.datos)
    Productor ─────────┤                     │
                        │      gold          │◀── cada 10 mensajes: media de
                        │                     │    temperatura y humedad por ciudad
                       └─────────────────────┘
```

| Capa | Contenido | Destino |
|---|---|---|
| **Bronze** | Respuesta completa de la API, sin transformar | MinIO, carpeta `bronze/` |
| **Silver** | Campos reducidos: ciudad, temperatura, humedad, fecha | MinIO (`silver/`) y MongoDB (`tiempo.datos`) |
| **Gold** | Media de temperatura y humedad cada 10 mensajes silver, agrupada por ciudad | Topic `gold` |

## Componentes

| Fichero | Función |
|---|---|
| `src/config.py` | Configuración compartida, leída de variables de entorno (`.env`) |
| `src/productor.py` | Consulta la API cada minuto y publica en los topics `bronze` y `silver` |
| `src/consumidor_bronze.py` | Lee el topic `bronze` y persiste cada mensaje tal cual en MinIO |
| `src/consumidor_silver.py` | Lee el topic `silver`, persiste en MinIO y MongoDB, y cada 10 mensajes calcula el agregado y lo publica en `gold` |

## Infraestructura (`docker-compose.yml`)

| Servicio | Imagen | Puerto | Función |
|---|---|---|---|
| `kafka` | apache/kafka:3.8.0 | 9094 | Broker único, topics bronze / silver / gold |
| `minio` | minio/minio | 9000 / 9001 | Almacenamiento tipo S3, bucket `tiempo` |
| `minio-init` | minio/mc | — | Crea el bucket `tiempo` automáticamente al arrancar |
| `mongo` | mongo:7 | 27017 | Base de datos `tiempo`, colección `datos` |
| `mongo-express` | mongo-express | 8081 | Interfaz web para explorar MongoDB |
| `kafka-ui` | provectuslabs/kafka-ui | 8080 | Interfaz web para explorar los topics de Kafka |

## Requisitos

- Docker y Docker Compose
- Python 3.10+

## Puesta en marcha

### 1. Levantar la infraestructura

```bash
docker compose up -d
```

### 2. Crear los topics de Kafka

```bash
docker exec -it --workdir /opt/kafka/bin iabd-kafka bash

./kafka-topics.sh --create --topic bronze --bootstrap-server kafka:9092
./kafka-topics.sh --create --topic silver --bootstrap-server kafka:9092
./kafka-topics.sh --create --topic gold --bootstrap-server kafka:9092
```

### 3. Instalar las dependencias de Python

```bash
pip install --break-system-packages confluent-kafka requests boto3 pymongo pandas python-dotenv
```

### 4. Configurar el entorno

Crear un fichero `.env` en la raíz del proyecto (no se sube al repositorio, ver `.gitignore`):

```
BOOTSTRAP_SERVERS=localhost:9094
TOPIC_BRONZE=bronze
TOPIC_SILVER=silver
TOPIC_GOLD=gold
AEMET_URL=https://api.el-tiempo.net/json/v3/provincias/03/municipios/03065
POLL_INTERVAL_SECONDS=60
S3_ENDPOINT=http://localhost:9000
S3_KEY=minioadmin
S3_SECRET=minioadmin123
S3_BUCKET=tiempo
MONGO_URI=mongodb://localhost:27017
MONGO_DB=tiempo
MONGO_COLLECTION=datos
GOLD_BATCH_SIZE=10
```

### 5. Ejecutar el pipeline

En tres terminales distintas, desde `src/`:

```bash
python3 productor.py
python3 consumidor_bronze.py
python3 consumidor_silver.py
```

## Verificación

**Topic gold** (mensajes agregados):

```bash
docker exec -it --workdir /opt/kafka/bin iabd-kafka \
  ./kafka-console-consumer.sh --topic gold --from-beginning --bootstrap-server kafka:9092
```

Ejemplo de salida:

```json
[{"ciudad": "Elche/Elx", "fecha": "2026-07-16T09:42:51.792883", "temp": 37.3, "humedad": 34.2}]
```

**Interfaces web** (requieren redirección de puertos si se accede desde fuera de la VM):

- MinIO: `http://localhost:9001` (usuario `minioadmin`, contraseña `minioadmin123`), bucket `tiempo`
- Mongo Express: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`

## Diseño y decisiones técnicas

- **Idempotencia**: los productores usan `acks: all` y `enable.idempotence: True` para evitar mensajes duplicados ante reintentos.
- **Grupos de consumidor independientes** (`grupo-bronze`, `grupo-silver`): permiten que ambos consumidores lean el mismo flujo de eventos de forma desacoplada, sin interferir entre sí.
- **`poll(0)` vs `flush()`**: los productores de larga duración llaman a `poll(0)` en cada iteración para procesar callbacks sin bloquear, y reservan `flush()` para el cierre.
- **Tipado del JSON silver**: al deserializar los mensajes se usa `object_hook` para convertir fecha, temperatura y humedad a sus tipos nativos antes de agregarlos con `pandas`.
- **Resiliencia**: los tres scripts manejan errores de red y de formato sin caerse (reintentan o descartan el mensaje problemático, según el caso), y los consumidores se recuperan automáticamente tras una caída temporal del broker.

## Posibles ampliaciones

- Serializar con **Avro** + Schema Registry en lugar de JSON.
- Persistir en formato **Parquet** en vez de JSON en la zona bronze/silver.
- Leer la zona bronze desde Spark: `spark.read.json("s3a://tiempo/bronze/")`.
