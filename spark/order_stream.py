import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import JDBC_PROPERTIES, JDBC_URL
from config.kafka import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from config.spark import (
    CHECKPOINT_ORDER_EVENTS,
    ORDER_EVENT_SCHEMA,
    SPARK_BATCH_TRIGGER,
    TABLE_SPARK_ORDER_EVENTS,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit


def write_batch_to_postgres(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty")
        return

    output_df = (
        batch_df
        .withColumn("spark_batch_id", lit(batch_id))
        .select(
            "event_id",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_total",
            "status",
            "city",
            "event_time",
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            "kafka_timestamp",
            "spark_batch_id",
        )
    )

    rows_count = output_df.count()

    (
        output_df.write
        .mode("append")
        .jdbc(
            url=JDBC_URL,
            table=TABLE_SPARK_ORDER_EVENTS,
            properties=JDBC_PROPERTIES,
        )
    )

    print(
        f"Batch {batch_id}: "
        f"written {rows_count} rows to PostgreSQL"
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("OrderEventsToPostgres")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("json_value"),
        )
        .withColumn(
            "event",
            from_json(col("json_value"), ORDER_EVENT_SCHEMA),
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event.*",
        )
        .filter(col("event_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
        .filter(col("line_total") >= 0)
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(write_batch_to_postgres)
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_ORDER_EVENTS,
        )
        .trigger(processingTime=SPARK_BATCH_TRIGGER)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
