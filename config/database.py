import os

from config.env import load_env

load_env()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5435")
DB_NAME = os.getenv("DB_NAME", "streaming_orders")
DB_USER = os.getenv("DB_USER", "streaming_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "streaming_pass")

JDBC_HOST = os.getenv("JDBC_HOST", "postgres")
JDBC_PORT = os.getenv("JDBC_PORT", "5432")
JDBC_DATABASE = os.getenv("JDBC_DATABASE", "streaming_orders")
JDBC_USER = os.getenv("JDBC_USER", "streaming_user")
JDBC_PASSWORD = os.getenv("JDBC_PASSWORD", "streaming_pass")

JDBC_URL = (
    f"jdbc:postgresql://{JDBC_HOST}:{JDBC_PORT}/{JDBC_DATABASE}"
)

JDBC_PROPERTIES = {
    "user": JDBC_USER,
    "password": JDBC_PASSWORD,
    "driver": "org.postgresql.Driver",
}
