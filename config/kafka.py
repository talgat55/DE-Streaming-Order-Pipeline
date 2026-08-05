import os

from config.env import load_env

load_env()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order-events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "order-events-postgres-consumer",
)
