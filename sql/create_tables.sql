CREATE TABLE IF NOT EXISTS  raw_order_events (
    id BIGSERIAL PRIMARY KEY,

    event_id UUID NOT NULL,
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,

    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    line_total NUMERIC(14, 2) NOT NULL,

    status TEXT NOT NULL,
    city TEXT,
    event_time TIMESTAMPTZ NOT NULL,

    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_raw_order_events_event_id
        UNIQUE (event_id),

    CONSTRAINT uq_raw_order_events_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset),

    CONSTRAINT chk_raw_order_events_quantity
        CHECK (quantity > 0),

    CONSTRAINT chk_raw_order_events_unit_price
        CHECK (unit_price >= 0),

    CONSTRAINT chk_raw_order_events_line_total
        CHECK (line_total >= 0)
)