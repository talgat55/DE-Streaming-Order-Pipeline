import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import JDBC_PROPERTIES, JDBC_URL
from config.kafka import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from config.spark import (
    CHECKPOINT_MINUTE_SALES,
    ORDER_EVENT_SCHEMA,
    TABLE_MINUTE_SALES_AGG,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    from_json,
    lit,
    sum as spark_sum,
    window,
)

UPSERT_SQL = f"""
    INSERT INTO {TABLE_MINUTE_SALES_AGG} (
        window_start,
        window_end,
        orders_count,
        items_count,
        revenue,
        spark_batch_id
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT (window_start, window_end) DO UPDATE SET
        orders_count = EXCLUDED.orders_count,
        items_count = EXCLUDED.items_count,
        revenue = EXCLUDED.revenue,
        spark_batch_id = EXCLUDED.spark_batch_id,
        loaded_at = NOW()
"""


def _upsert_rows(rows, spark_session) -> None:
    jvm = spark_session._jvm
    jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(
        JDBC_PROPERTIES["driver"],
    )

    conn = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        JDBC_PROPERTIES["user"],
        JDBC_PROPERTIES["password"],
    )
    stmt = None
    try:
        conn.setAutoCommit(False)
        stmt = conn.prepareStatement(UPSERT_SQL)
        for row in rows:
            stmt.setTimestamp(
                1,
                jvm.java.sql.Timestamp(int(row.window_start.timestamp() * 1000)),
            )
            stmt.setTimestamp(
                2,
                jvm.java.sql.Timestamp(int(row.window_end.timestamp() * 1000)),
            )
            stmt.setLong(3, int(row.orders_count))
            stmt.setLong(4, int(row.items_count))
            stmt.setBigDecimal(
                5,
                jvm.java.math.BigDecimal(str(row.revenue)),
            )
            stmt.setLong(6, int(row.spark_batch_id))
            stmt.addBatch()
        stmt.executeBatch()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if stmt is not None:
            stmt.close()
        conn.close()


def write_aggregates(
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
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "orders_count",
            "items_count",
            "revenue",
            "spark_batch_id",
        )
    )

    rows = output_df.collect()
    _upsert_rows(rows, batch_df.sparkSession)

    print(f"Batch {batch_id}: upserted {len(rows)} aggregates")

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("MinuteSalesStreaming")
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
        .option('subscribe', KAFKA_TOPIC)
        .option('startingOffsets', "earliest")
        .load()
    )

    parsed_stram = (
        kafka_stream
        .select(
            col("value").cast("string").alias("json_value")
        )
        .withColumn(
            "event",
            from_json(col("json_value"), ORDER_EVENT_SCHEMA)
        )
        .select("event.*")
        .filter(col("event_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("line_total") >= 0)
        .filter(col("status") == "paid")
    )

    aggregates = (
        parsed_stram
            .withWatermark("event_time", "2 minutes")
            .groupBy(
                window(
                    col("event_time"),
                    "1 minute"
                )
            )
            .agg(
                approx_count_distinct("order_id").alias("orders_count"),
                spark_sum("quantity").alias("items_count"),
                spark_sum("line_total").alias("revenue"),
            )
    )

    query = (
        aggregates.writeStream
        .foreachBatch(write_aggregates)
        .outputMode("update")
        .option(
            "checkpointLocation",
            CHECKPOINT_MINUTE_SALES
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()


