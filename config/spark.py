import os

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CHECKPOINT_BASE = os.getenv(
    "SPARK_CHECKPOINT_BASE",
    "/opt/spark-checkpoints",
)
CHECKPOINT_ORDER_EVENTS = (
    f"{CHECKPOINT_BASE}/order-events-postgres"
)
CHECKPOINT_MINUTE_SALES = f"{CHECKPOINT_BASE}/minute-sales"

TABLE_SPARK_ORDER_EVENTS = "spark_order_events"
TABLE_MINUTE_SALES_AGG = "minute_sales_agg"

SPARK_BATCH_TRIGGER = os.getenv("SPARK_BATCH_TRIGGER", "5 seconds")

ORDER_EVENT_SCHEMA = StructType([
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
