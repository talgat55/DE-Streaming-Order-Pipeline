import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "order-events"
CONSUMER_GROUP = "order-events-debug-consumer"

def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

def main() -> None:
    consumer = create_consumer()

    print(
        f"Consumer started. "
        f"Topic={KAFKA_TOPIC}, group={CONSUMER_GROUP}"
    )

    try:
        for message in consumer:
            event = message.value

            print(
                f"partition={message.partition}"
                f"offset={message.offset}"
                f"key={message.key}"
                f"order_id={event.get('order_id')}"
                f"status={event.get('status')}"
                f"line_total={event.get('line_total')}"
            )
    except KeyboardInterrupt:
        print('Consumer stopped')

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
