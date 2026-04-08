import os
import time
import logging
import psycopg2

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRANSFORMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST              = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT              = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB                = os.getenv("POSTGRES_DB", "crypto_db")
PG_USER              = os.getenv("POSTGRES_USER", "crypto_user")
PG_PASSWORD          = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
TRANSFORM_INTERVAL   = int(os.getenv("TRANSFORM_INTERVAL_SECONDS", "60"))

# ── Bronze → Silver ───────────────────────────────────────────────────────────
# Cast TEXT fields to proper types, drop nulls and duplicates
BRONZE_TO_SILVER = """
INSERT INTO silver.crypto_ticker (
    symbol, price_change, price_change_pct, weighted_avg_price,
    prev_close_price, last_price, last_qty, bid_price, ask_price,
    open_price, high_price, low_price, volume, quote_volume,
    open_time, close_time, trade_count, bronze_ingested_at
)
SELECT
    b.symbol,
    NULLIF(b.price_change, '')::NUMERIC,
    NULLIF(b.price_change_pct, '')::NUMERIC,
    NULLIF(b.weighted_avg_price, '')::NUMERIC,
    NULLIF(b.prev_close_price, '')::NUMERIC,
    NULLIF(b.last_price, '')::NUMERIC,
    NULLIF(b.last_qty, '')::NUMERIC,
    NULLIF(b.bid_price, '')::NUMERIC,
    NULLIF(b.ask_price, '')::NUMERIC,
    NULLIF(b.open_price, '')::NUMERIC,
    NULLIF(b.high_price, '')::NUMERIC,
    NULLIF(b.low_price, '')::NUMERIC,
    NULLIF(b.volume, '')::NUMERIC,
    NULLIF(b.quote_volume, '')::NUMERIC,
    TO_TIMESTAMP(b.open_time / 1000.0),
    TO_TIMESTAMP(b.close_time / 1000.0),
    b.trade_count,
    b.ingested_at
FROM bronze.crypto_ticker_raw b
WHERE
    -- Only process records not yet in Silver
    b.id > (SELECT COALESCE(MAX(id), 0) FROM silver._watermark)
    -- Drop records with null critical fields
    AND b.symbol IS NOT NULL
    AND NULLIF(b.last_price, '') IS NOT NULL
    AND NULLIF(b.volume, '') IS NOT NULL
ON CONFLICT DO NOTHING;
"""

# Update the watermark after Silver load
UPDATE_WATERMARK = """
INSERT INTO silver._watermark (id)
SELECT MAX(id) FROM bronze.crypto_ticker_raw
ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id;
"""

# ── Silver → Gold: latest prices ─────────────────────────────────────────────
SILVER_TO_GOLD_LATEST = """
INSERT INTO gold.latest_prices (
    symbol, last_price, open_price, high_price, low_price,
    price_change, price_change_pct, volume, quote_volume,
    trade_count, updated_at
)
SELECT DISTINCT ON (symbol)
    symbol, last_price, open_price, high_price, low_price,
    price_change, price_change_pct, volume, quote_volume,
    trade_count, transformed_at
FROM silver.crypto_ticker
ORDER BY symbol, transformed_at DESC
ON CONFLICT (symbol) DO UPDATE SET
    last_price       = EXCLUDED.last_price,
    open_price       = EXCLUDED.open_price,
    high_price       = EXCLUDED.high_price,
    low_price        = EXCLUDED.low_price,
    price_change     = EXCLUDED.price_change,
    price_change_pct = EXCLUDED.price_change_pct,
    volume           = EXCLUDED.volume,
    quote_volume     = EXCLUDED.quote_volume,
    trade_count      = EXCLUDED.trade_count,
    updated_at       = EXCLUDED.updated_at;
"""

# ── Silver → Gold: top gainers ────────────────────────────────────────────────
SILVER_TO_GOLD_GAINERS = """
DELETE FROM gold.top_gainers;
INSERT INTO gold.top_gainers (rank, symbol, last_price, price_change_pct, volume, updated_at)
SELECT
    ROW_NUMBER() OVER (ORDER BY price_change_pct DESC) AS rank,
    symbol, last_price, price_change_pct, volume, NOW()
FROM gold.latest_prices
WHERE price_change_pct IS NOT NULL
ORDER BY price_change_pct DESC
LIMIT 20;
"""

# ── Silver → Gold: top losers ─────────────────────────────────────────────────
SILVER_TO_GOLD_LOSERS = """
DELETE FROM gold.top_losers;
INSERT INTO gold.top_losers (rank, symbol, last_price, price_change_pct, volume, updated_at)
SELECT
    ROW_NUMBER() OVER (ORDER BY price_change_pct ASC) AS rank,
    symbol, last_price, price_change_pct, volume, NOW()
FROM gold.latest_prices
WHERE price_change_pct IS NOT NULL
ORDER BY price_change_pct ASC
LIMIT 20;
"""

# ── Silver → Gold: volume leaders ────────────────────────────────────────────
SILVER_TO_GOLD_VOLUME = """
DELETE FROM gold.volume_leaders;
INSERT INTO gold.volume_leaders (rank, symbol, last_price, quote_volume, price_change_pct, trade_count, updated_at)
SELECT
    ROW_NUMBER() OVER (ORDER BY quote_volume DESC) AS rank,
    symbol, last_price, quote_volume, price_change_pct, trade_count, NOW()
FROM gold.latest_prices
WHERE quote_volume IS NOT NULL
ORDER BY quote_volume DESC
LIMIT 20;
"""

# ── Silver → Gold: hourly OHLCV summary ──────────────────────────────────────
SILVER_TO_GOLD_HOURLY = """
INSERT INTO gold.hourly_summary (
    symbol, hour, open_price, high_price, low_price, close_price,
    total_volume, total_quote_volume, avg_price_change_pct, record_count
)
SELECT
    symbol,
    DATE_TRUNC('hour', transformed_at) AS hour,
    MIN(open_price)          AS open_price,
    MAX(high_price)          AS high_price,
    MIN(low_price)           AS low_price,
    MAX(last_price)          AS close_price,
    SUM(volume)              AS total_volume,
    SUM(quote_volume)        AS total_quote_volume,
    AVG(price_change_pct)    AS avg_price_change_pct,
    COUNT(*)                 AS record_count
FROM silver.crypto_ticker
WHERE transformed_at >= DATE_TRUNC('hour', NOW()) - INTERVAL '2 hours'
  AND transformed_at <  DATE_TRUNC('hour', NOW())
GROUP BY symbol, DATE_TRUNC('hour', transformed_at)
ON CONFLICT (symbol, hour) DO UPDATE SET
    high_price           = GREATEST(gold.hourly_summary.high_price, EXCLUDED.high_price),
    low_price            = LEAST(gold.hourly_summary.low_price, EXCLUDED.low_price),
    close_price          = EXCLUDED.close_price,
    total_volume         = EXCLUDED.total_volume,
    total_quote_volume   = EXCLUDED.total_quote_volume,
    avg_price_change_pct = EXCLUDED.avg_price_change_pct,
    record_count         = EXCLUDED.record_count;
"""

def create_pg_connection(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )
            conn.autocommit = False
            log.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("Postgres not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL.")


def ensure_watermark_table(cursor):
    """Create a single-row watermark table to track Bronze → Silver progress."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver._watermark (
            id BIGINT PRIMARY KEY DEFAULT 0
        );
        INSERT INTO silver._watermark (id) VALUES (0) ON CONFLICT DO NOTHING;
    """)


def run_transform(cursor):
    log.info("── Starting transformation cycle ──────────────────")

    # Bronze → Silver
    cursor.execute(BRONZE_TO_SILVER)
    silver_rows = cursor.rowcount
    cursor.execute(UPDATE_WATERMARK)
    log.info("🥈 Silver: %d new records transformed from Bronze", silver_rows)

    # Silver → Gold
    cursor.execute(SILVER_TO_GOLD_LATEST)
    log.info("🥇 Gold: latest_prices refreshed")

    cursor.execute(SILVER_TO_GOLD_GAINERS)
    log.info("🥇 Gold: top_gainers refreshed")

    cursor.execute(SILVER_TO_GOLD_LOSERS)
    log.info("🥇 Gold: top_losers refreshed")

    cursor.execute(SILVER_TO_GOLD_VOLUME)
    log.info("🥇 Gold: volume_leaders refreshed")

    cursor.execute(SILVER_TO_GOLD_HOURLY)
    log.info("🥇 Gold: hourly_summary refreshed")

    log.info("── Transformation cycle complete ───────────────────")


def main():
    conn   = create_pg_connection()
    cursor = conn.cursor()
    ensure_watermark_table(cursor)
    conn.commit()

    log.info("Transformer running — every %d seconds", TRANSFORM_INTERVAL)
    while True:
        try:
            run_transform(cursor)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error("Transformation failed: %s", e)

        time.sleep(TRANSFORM_INTERVAL)


if __name__ == "__main__":
    main()
