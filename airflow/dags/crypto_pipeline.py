"""
Crypto Data Pipeline DAG
========================
Orchestrates the full Medallion Architecture pipeline:

Schedule: every 5 minutes

Tasks:
  1. check_kafka_health        — verify Kafka broker is reachable
  2. check_postgres_health     — verify Postgres is reachable
  3. fetch_binance_data        — trigger producer to fetch from Binance API
  4. validate_bronze_ingestion — confirm new rows landed in Bronze
  5. run_bronze_to_silver      — trigger transformer Bronze → Silver
  6. validate_silver_data      — check Silver row counts and nulls
  7. run_silver_to_gold        — trigger transformer Silver → Gold
  8. validate_gold_data        — confirm Gold tables are populated
  9. update_pipeline_health    — write health summary to gold.pipeline_health
 10. alert_on_errors           — check DLQ and error_log, log warnings
"""

from datetime import datetime, timedelta
import logging
import json
import requests
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── Default args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "crypto-pipeline",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(seconds=30),
}

# ── Postgres connection helper ────────────────────────────────────────────────
def get_pg_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="crypto_db",
        user="crypto_user",
        password="crypto_pass",
    )


# ── Task functions ────────────────────────────────────────────────────────────

def check_kafka_health(**ctx):
    """Ping Kafka broker via REST proxy or TCP check."""
    import socket
    try:
        sock = socket.create_connection(("kafka", 29092), timeout=5)
        sock.close()
        log.info("✅ Kafka broker reachable at kafka:29092")
    except Exception as e:
        raise RuntimeError(f"Kafka health check failed: {e}")


def check_postgres_health(**ctx):
    """Verify Postgres connection and all 3 schemas exist."""
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name IN ('bronze', 'silver', 'gold')
        ORDER BY schema_name;
    """)
    schemas = [row[0] for row in cursor.fetchall()]
    conn.close()
    if len(schemas) < 3:
        raise RuntimeError(f"Missing schemas — found: {schemas}, expected: bronze, silver, gold")
    log.info("✅ Postgres healthy — schemas found: %s", schemas)


def validate_bronze_ingestion(**ctx):
    """Check that Bronze received new rows in the last 10 minutes."""
    conn   = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM bronze.crypto_ticker_raw
        WHERE ingested_at >= NOW() - INTERVAL '10 minutes'
    """)
    count = cursor.fetchone()[0]
    conn.close()

    if count == 0:
        raise RuntimeError("Bronze validation FAILED — no new rows in last 10 minutes. Producer may be down.")

    log.info("✅ Bronze validation passed — %d new rows in last 10 minutes", count)
    ctx["ti"].xcom_push(key="bronze_new_rows", value=count)


def run_bronze_to_silver(**ctx):
    """Execute Bronze → Silver transformation via direct SQL."""
    conn   = get_pg_conn()
    cursor = conn.cursor()

    cursor.execute("""
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
        WHERE b.id > (SELECT COALESCE(MAX(id), 0) FROM silver._watermark)
          AND b.symbol IS NOT NULL
          AND NULLIF(b.last_price, '') IS NOT NULL
          AND NULLIF(b.volume, '') IS NOT NULL
        ON CONFLICT DO NOTHING;
    """)

    silver_rows = cursor.rowcount

    cursor.execute("""
        INSERT INTO silver._watermark (id)
        SELECT MAX(id) FROM bronze.crypto_ticker_raw
        ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id;
    """)

    conn.commit()
    conn.close()

    log.info("🥈 Silver: %d new rows transformed from Bronze", silver_rows)
    ctx["ti"].xcom_push(key="silver_new_rows", value=silver_rows)


def validate_silver_data(**ctx):
    """Validate Silver — check for nulls in critical fields."""
    conn   = get_pg_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE last_price IS NULL) AS null_prices,
            COUNT(*) FILTER (WHERE symbol IS NULL)     AS null_symbols,
            COUNT(*) FILTER (WHERE volume IS NULL)     AS null_volumes
        FROM silver.crypto_ticker
        WHERE transformed_at >= NOW() - INTERVAL '10 minutes'
    """)
    row = cursor.fetchone()
    conn.close()

    total, null_prices, null_symbols, null_volumes = row
    log.info("🥈 Silver quality: total=%d | null_prices=%d | null_symbols=%d | null_volumes=%d",
             total, null_prices, null_symbols, null_volumes)

    if total == 0:
        raise RuntimeError("Silver validation FAILED — no recent rows found")
    if null_symbols > 0:
        raise RuntimeError(f"Silver validation FAILED — {null_symbols} null symbols found")


def run_silver_to_gold(**ctx):
    """Execute Silver → Gold transformation."""
    conn   = get_pg_conn()
    cursor = conn.cursor()

    # Latest prices
    cursor.execute("""
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
    """)

    # Top gainers
    cursor.execute("DELETE FROM gold.top_gainers;")
    cursor.execute("""
        INSERT INTO gold.top_gainers (rank, symbol, last_price, price_change_pct, volume, updated_at)
        SELECT ROW_NUMBER() OVER (ORDER BY price_change_pct DESC),
               symbol, last_price, price_change_pct, volume, NOW()
        FROM gold.latest_prices
        WHERE price_change_pct IS NOT NULL
        ORDER BY price_change_pct DESC LIMIT 20;
    """)

    # Top losers
    cursor.execute("DELETE FROM gold.top_losers;")
    cursor.execute("""
        INSERT INTO gold.top_losers (rank, symbol, last_price, price_change_pct, volume, updated_at)
        SELECT ROW_NUMBER() OVER (ORDER BY price_change_pct ASC),
               symbol, last_price, price_change_pct, volume, NOW()
        FROM gold.latest_prices
        WHERE price_change_pct IS NOT NULL
        ORDER BY price_change_pct ASC LIMIT 20;
    """)

    # Volume leaders
    cursor.execute("DELETE FROM gold.volume_leaders;")
    cursor.execute("""
        INSERT INTO gold.volume_leaders (rank, symbol, last_price, quote_volume, price_change_pct, trade_count, updated_at)
        SELECT ROW_NUMBER() OVER (ORDER BY quote_volume DESC),
               symbol, last_price, quote_volume, price_change_pct, trade_count, NOW()
        FROM gold.latest_prices
        WHERE quote_volume IS NOT NULL
        ORDER BY quote_volume DESC LIMIT 20;
    """)

    # Hourly summary
    cursor.execute("""
        INSERT INTO gold.hourly_summary (
            symbol, hour, open_price, high_price, low_price, close_price,
            total_volume, total_quote_volume, avg_price_change_pct, record_count
        )
        SELECT
            symbol,
            DATE_TRUNC('hour', transformed_at) AS hour,
            MIN(open_price), MAX(high_price), MIN(low_price), MAX(last_price),
            SUM(volume), SUM(quote_volume), AVG(price_change_pct), COUNT(*)
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
    """)

    conn.commit()
    conn.close()
    log.info("🥇 Gold: all tables refreshed successfully")


def validate_gold_data(**ctx):
    """Confirm Gold tables have data."""
    conn   = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM gold.latest_prices)  AS latest,
            (SELECT COUNT(*) FROM gold.top_gainers)    AS gainers,
            (SELECT COUNT(*) FROM gold.top_losers)     AS losers,
            (SELECT COUNT(*) FROM gold.volume_leaders) AS volume
    """)
    row = cursor.fetchone()
    conn.close()

    latest, gainers, losers, volume = row
    log.info("🥇 Gold validation: latest_prices=%d | gainers=%d | losers=%d | volume=%d",
             latest, gainers, losers, volume)

    if latest == 0:
        raise RuntimeError("Gold validation FAILED — latest_prices table is empty")


def update_pipeline_health(**ctx):
    """Write a health snapshot to gold.pipeline_health."""
    conn   = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO gold.pipeline_health (bronze_count, silver_count, gold_count, error_count, dlq_count)
        SELECT
            (SELECT COUNT(*) FROM bronze.crypto_ticker_raw),
            (SELECT COUNT(*) FROM silver.crypto_ticker),
            (SELECT COUNT(*) FROM gold.latest_prices),
            (SELECT COUNT(*) FROM bronze.error_log),
            (SELECT COUNT(*) FROM bronze.error_log WHERE source = 'dlq_consumer')
    """)
    conn.commit()
    conn.close()
    log.info("📊 Pipeline health snapshot written to gold.pipeline_health")


def alert_on_errors(**ctx):
    """Check error counts and log warnings if DLQ has recent activity."""
    conn   = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT error_type, COUNT(*) AS cnt
        FROM bronze.error_log
        WHERE occurred_at >= NOW() - INTERVAL '10 minutes'
        GROUP BY error_type
        ORDER BY cnt DESC
    """)
    errors = cursor.fetchall()
    conn.close()

    if errors:
        for error_type, count in errors:
            log.warning("⚠️  Recent errors — type: %s | count: %d", error_type, count)
        total = sum(c for _, c in errors)
        if total > 100:
            raise RuntimeError(f"Too many errors in last 10 minutes: {total}. Check DLQ!")
    else:
        log.info("✅ No errors in the last 10 minutes")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="crypto_pipeline",
    description="Crypto Medallion Architecture Pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",   # every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "medallion", "kafka", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_kafka_health    = PythonOperator(task_id="check_kafka_health",        python_callable=check_kafka_health)
    t_pg_health       = PythonOperator(task_id="check_postgres_health",     python_callable=check_postgres_health)
    t_validate_bronze = PythonOperator(task_id="validate_bronze_ingestion", python_callable=validate_bronze_ingestion)
    t_bronze_silver   = PythonOperator(task_id="run_bronze_to_silver",      python_callable=run_bronze_to_silver)
    t_validate_silver = PythonOperator(task_id="validate_silver_data",      python_callable=validate_silver_data)
    t_silver_gold     = PythonOperator(task_id="run_silver_to_gold",        python_callable=run_silver_to_gold)
    t_validate_gold   = PythonOperator(task_id="validate_gold_data",        python_callable=validate_gold_data)
    t_health          = PythonOperator(task_id="update_pipeline_health",    python_callable=update_pipeline_health)
    t_alert           = PythonOperator(task_id="alert_on_errors",           python_callable=alert_on_errors)

    # ── Task dependencies ─────────────────────────────────────────────────────
    start >> [t_kafka_health, t_pg_health]
    [t_kafka_health, t_pg_health] >> t_validate_bronze
    t_validate_bronze >> t_bronze_silver
    t_bronze_silver >> t_validate_silver
    t_validate_silver >> t_silver_gold
    t_silver_gold >> t_validate_gold
    t_validate_gold >> [t_health, t_alert]
    [t_health, t_alert] >> end
