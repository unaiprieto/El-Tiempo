import os
from dotenv import load_dotenv

load_dotenv()


def _env(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Falta la variable de entorno obligatoria: {name}")
    return value


BOOTSTRAP_SERVERS = _env("BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_BRONZE = _env("TOPIC_BRONZE", "bronze")
TOPIC_SILVER = _env("TOPIC_SILVER", "silver")
TOPIC_GOLD = _env("TOPIC_GOLD", "gold")

AEMET_URL = _env("AEMET_URL", "https://api.el-tiempo.net/json/v3/provincias/03/municipios/03065")
POLL_INTERVAL_SECONDS = int(_env("POLL_INTERVAL_SECONDS", "60"))

S3_ENDPOINT = os.getenv("S3_ENDPOINT") or None
S3_KEY = os.getenv("S3_KEY")
S3_SECRET = os.getenv("S3_SECRET")
S3_BUCKET = _env("S3_BUCKET", "tiempo")

MONGO_URI = _env("MONGO_URI")
MONGO_DB = _env("MONGO_DB", "tiempo")
MONGO_COLLECTION = _env("MONGO_COLLECTION", "datos")

GOLD_BATCH_SIZE = int(_env("GOLD_BATCH_SIZE", "10"))


def s3_resource():
    import boto3
    kwargs = {"region_name": "us-east-1"}
    if S3_ENDPOINT:
        kwargs.update(endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET)
    return boto3.resource("s3", **kwargs)
