from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "order-events"

CHECKPOINT_LOCATION = "/opt/spark-checkpoints/order-events-console"

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("order_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("product_id", LongType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(12, 2), False),
    StructField("line_total", DecimalType(14, 2), False),
    StructField("status", StringType(), False),
    StructField("city", StringType(), False),
    StructField("event_time", TimestampType(), False),
])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("OrderEventsStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
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
        .withColumn("event", from_json(col("json_value"), event_schema))
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event.*",
        )
        .filter(col("event_id").isNotNull())
    )

    query = (
        parsed_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
