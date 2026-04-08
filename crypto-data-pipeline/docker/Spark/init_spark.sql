-- ============================================================
--  Add this to your existing init.sql
--  GOLD — Spark Streaming Aggregations table
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.spark_aggregations (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(20)  NOT NULL,
    window_start        TIMESTAMP WITH TIME ZONE,
    window_end          TIMESTAMP WITH TIME ZONE,
    avg_price           NUMERIC(20, 8),
    max_price           NUMERIC(20, 8),
    min_price           NUMERIC(20, 8),
    total_volume        NUMERIC(30, 8),
    avg_price_change_pct NUMERIC(10, 4),
    record_count        BIGINT,
    computed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_spark_agg_symbol      ON gold.spark_aggregations(symbol);
CREATE INDEX IF NOT EXISTS idx_spark_agg_window_start ON gold.spark_aggregations(window_start);
