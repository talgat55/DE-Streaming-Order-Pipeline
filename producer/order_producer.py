import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "order-events"

fake = Faker()

def create_order_event() -> dict:
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 1000), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": random.randint(1, 100_000),
        "customer_id": random.randint(1, 100_000),
        "product_id": random.randint(1, 100_000),
        "quantity": quantity,
        "unit_price": unit_price,
        "linet_total": round(quantity * unit_price, 2),
        "status": random.choice([
            "created",
            "paid",
            "shipped",
            "delivered",
            "cancelled",
        ]),
        "city": fake.city(),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )

def main() -> None:
    producer = create_producer()

    print(f"Producer started. Topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = create_order_event()

            future = producer.send(
                KAFKA_TOPIC,
                key=str(event["order_id"]),
                value=event
            )

            metadata = future.get(timeout=10)

            print(
                f"Send event_id={event['event_id']}"
                f"order_id={event['order_id']}"
                f"partition={metadata.partition}"
                f"offset={metadata.offset}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("Producer stopped")

    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()