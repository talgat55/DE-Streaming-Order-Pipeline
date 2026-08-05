import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from typing import Any
from kafka import KafkaConsumer
from config.kafka import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
)
from db import get_connection

def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

def save_event(
        event: dict[str, Any],
        topic: str,
        partition: int,
        offset: int
) -> bool:
    sql = """
        INSERT INTO raw_order_events (
            event_id,
            order_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            line_total,
            status
            city,
            event_time,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING;
    """

    connection = get_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        event["event_id"],
                        event["order_id"],
                        event["customer_id"],
                        event["product_id"],
                        event["quantity"],
                        event["unit_price"],
                        event["line_total"],
                        event["status"],
                        event.get("city"),
                        topic,
                        partition,
                        offset,
                    )
                )

                return cursor.rowcount == 1
    finally:
        connection.close()

def main() -> None:
    consumer = create_consumer()

    print(
        f"Consumer started. "
        f"Topic={KAFKA_TOPIC}, group={KAFKA_CONSUMER_GROUP}"
    )

    try:
        for message in consumer:
            event = message.value

            try:
                inserted = save_event(
                    event=event,
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                )

                # Offset подтверждаем только после успешной транзакции БД.
                consumer.commit()

                action = "inserted" if inserted else "duplicate_skipped"

                print(
                    f"{action} "
                    f"event_id={event.get('event_id')} "
                    f"partition={message.partition} "
                    f"offset={message.offset}"
                )

            except (KeyError, TypeError, ValueError) as error:
                print(
                    f"Invalid event: {error}; "
                    f"partition={message.partition}; "
                    f"offset={message.offset}; "
                    f"value={event}"
                )

                # Пока плохое событие пропускаем.
                consumer.commit()

            except Exception as error:
                print(
                    f"Database error: {error}; "
                    f"partition={message.partition}; "
                    f"offset={message.offset}"
                )

                # Offset не подтверждаем.
                # После перезапуска Kafka отдаст событие повторно.
                raise

    except KeyboardInterrupt:
        print('Consumer stopped')

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
