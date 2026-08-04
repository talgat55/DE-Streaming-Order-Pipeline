from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit
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

CHECKPOINT_LOCATION = "/opt/spark-checkpoints/order-events-postgres"

JDBC_URL = "jdbc:postgresql://postgres:5432/streaming_orders"
JDBC_TABLE = "spark_order_events"

JDBC_PROPERTIES = {
    "user": "streaming_user",
    "password": "streaming_pass",
    "driver": "org.postgresql.Driver",
}


event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("order_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("product_id", LongType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(12, 2), False),
    StructField("line_total", DecimalType(14, 2), False),
    StructField("status", StringType(), False),
    StructField("city", StringType(), True),
    StructField("event_time", TimestampType(), False),
])


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
            table=JDBC_TABLE,
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
            from_json(col("json_value"), event_schema),
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
            CHECKPOINT_LOCATION,
        )
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()