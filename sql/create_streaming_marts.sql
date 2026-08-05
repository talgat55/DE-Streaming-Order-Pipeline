CREATE TABLE IF NOT EXISTS minute_sales_agg (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,

    orders_count BIGINT NOT NULL,
    items_count BIGINT NOT NULL,
    revenue NUMERIC(18, 2) NOT NULL,

    spark_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_minute_sales_agg_window
        UNIQUE (window_start, window_end)
)