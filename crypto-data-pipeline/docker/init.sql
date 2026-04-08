-- ============================================================
--  MEDALLION ARCHITECTURE — Bronze / Silver / Gold
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
--  BRONZE — Raw data
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.crypto_ticker_raw (
    id                  SERIAL PRIMARY KEY,
    symbol              TEXT,
    price_change        TEXT,
    price_change_pct    TEXT,
    weighted_avg_price  TEXT,
    prev_close_price    TEXT,
    last_price          TEXT,
    last_qty            TEXT,
    bid_price           TEXT,
    ask_price           TEXT,
    open_price          TEXT,
    high_price          TEXT,
    low_price           TEXT,
    volume              TEXT,
    quote_volume        TEXT,
    open_time           BIGINT,
    close_time          BIGINT,
    trade_count         BIGINT,
    ingested_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_symbol      ON bronze.crypto_ticker_raw(symbol);
CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at ON bronze.crypto_ticker_raw(ingested_at);

-- ============================================================
--  BRONZE — Error log (DLQ mirror in Postgres)
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.error_log (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,  -- 'consumer', 'transformer', 'producer'
    error_type      VARCHAR(100) NOT NULL,  -- 'validation', 'parse', 'db_insert', etc.
    error_message   TEXT         NOT NULL,
    raw_payload     TEXT,                   -- original raw message that failed
    kafka_topic     VARCHAR(100),
    kafka_partition INT,
    kafka_offset    BIGINT,
    occurred_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_error_log_source     ON bronze.error_log(source);
CREATE INDEX IF NOT EXISTS idx_error_log_error_type ON bronze.error_log(error_type);
CREATE INDEX IF NOT EXISTS idx_error_log_occurred_at ON bronze.error_log(occurred_at);

-- ============================================================
--  SILVER — Cleaned & typed
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.crypto_ticker (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(20)  NOT NULL,
    price_change        NUMERIC(20, 8),
    price_change_pct    NUMERIC(10, 4),
    weighted_avg_price  NUMERIC(20, 8),
    prev_close_price    NUMERIC(20, 8),
    last_price          NUMERIC(20, 8),
    last_qty            NUMERIC(20, 8),
    bid_price           NUMERIC(20, 8),
    ask_price           NUMERIC(20, 8),
    open_price          NUMERIC(20, 8),
    high_price          NUMERIC(20, 8),
    low_price           NUMERIC(20, 8),
    volume              NUMERIC(30, 8),
    quote_volume        NUMERIC(30, 8),
    open_time           TIMESTAMP WITH TIME ZONE,
    close_time          TIMESTAMP WITH TIME ZONE,
    trade_count         BIGINT,
    bronze_ingested_at  TIMESTAMP WITH TIME ZONE,
    transformed_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_symbol         ON silver.crypto_ticker(symbol);
CREATE INDEX IF NOT EXISTS idx_silver_transformed_at ON silver.crypto_ticker(transformed_at);

-- Watermark table for Bronze → Silver tracking
CREATE TABLE IF NOT EXISTS silver._watermark (
    id BIGINT PRIMARY KEY DEFAULT 0
);
INSERT INTO silver._watermark (id) VALUES (0) ON CONFLICT DO NOTHING;

-- ============================================================
--  GOLD — Aggregated tables
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.latest_prices (
    symbol              VARCHAR(20) PRIMARY KEY,
    last_price          NUMERIC(20, 8),
    open_price          NUMERIC(20, 8),
    high_price          NUMERIC(20, 8),
    low_price           NUMERIC(20, 8),
    price_change        NUMERIC(20, 8),
    price_change_pct    NUMERIC(10, 4),
    volume              NUMERIC(30, 8),
    quote_volume        NUMERIC(30, 8),
    trade_count         BIGINT,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.top_gainers (
    rank                INT,
    symbol              VARCHAR(20),
    last_price          NUMERIC(20, 8),
    price_change_pct    NUMERIC(10, 4),
    volume              NUMERIC(30, 8),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.top_losers (
    rank                INT,
    symbol              VARCHAR(20),
    last_price          NUMERIC(20, 8),
    price_change_pct    NUMERIC(10, 4),
    volume              NUMERIC(30, 8),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.volume_leaders (
    rank                INT,
    symbol              VARCHAR(20),
    last_price          NUMERIC(20, 8),
    quote_volume        NUMERIC(30, 8),
    price_change_pct    NUMERIC(10, 4),
    trade_count         BIGINT,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.hourly_summary (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(20),
    hour                TIMESTAMP WITH TIME ZONE,
    open_price          NUMERIC(20, 8),
    high_price          NUMERIC(20, 8),
    low_price           NUMERIC(20, 8),
    close_price         NUMERIC(20, 8),
    total_volume        NUMERIC(30, 8),
    total_quote_volume  NUMERIC(30, 8),
    avg_price_change_pct NUMERIC(10, 4),
    record_count        INT,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_hourly_symbol_hour ON gold.hourly_summary(symbol, hour);
CREATE INDEX IF NOT EXISTS idx_gold_hourly_hour ON gold.hourly_summary(hour);

-- ============================================================
--  GOLD — Pipeline health summary (for Grafana)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.pipeline_health (
    id              SERIAL PRIMARY KEY,
    checked_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    bronze_count    BIGINT,
    silver_count    BIGINT,
    gold_count      BIGINT,
    error_count     BIGINT,
    dlq_count       BIGINT
);
