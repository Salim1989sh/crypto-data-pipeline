import os
import json
import time
import logging
import psycopg2
from psycopg2.extras import execute_batch
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC", "crypto-ticker")
KAFKA_DLQ_TOPIC          = os.getenv("KAFKA_DLQ_TOPIC", "crypto-ticker-dlq")
KAFKA_GROUP_ID           = os.getenv("KAFKA_GROUP_ID", "crypto-consumer-group")
SCHEMA_REGISTRY_URL      = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
PG_HOST                  = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT                  = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB                    = os.getenv("POSTGRES_DB", "crypto_db")
PG_USER                  = os.getenv("POSTGRES_USER", "crypto_user")
PG_PASSWORD              = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
BATCH_SIZE               = int(os.getenv("BATCH_SIZE", "100"))

# Required fields for validation
REQUIRED_FIELDS = ["symbol", "lastPrice", "volume"]

# ── SQL ───────────────────────────────────────────────────────────────────────
INSERT_BRONZE = """
INSERT INTO bronze.crypto_ticker_raw (
    symbol, price_change, price_change_pct, weighted_avg_price,
    prev_close_price, last_price, last_qty, bid_price, ask_price,
    open_price, high_price, low_price, volume, quote_volume,
    open_time, close_time, trade_count
) VALUES (
    %(symbol)s, %(priceChange)s, %(priceChangePercent)s, %(weightedAvgPrice)s,
    %(prevClosePrice)s, %(lastPrice)s, %(lastQty)s, %(bidPrice)s, %(askPrice)s,
    %(openPrice)s, %(highPrice)s, %(lowPrice)s, %(volume)s, %(quoteVolume)s,
    %(openTime)s, %(closeTime)s, %(count)s
)
"""

INSERT_ERROR = """
INSERT INTO bronze.error_log (
    source, error_type, error_message, raw_payload,
    kafka_topic, kafka_partition, kafka_offset
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


# ── Validation ────────────────────────────────────────────────────────────────
def validate_record(record: dict) -> tuple[bool, str]:
    for field in REQUIRED_FIELDS:
        if not record.get(field):
            return False, f"Missing required field: '{field}'"
    try:
        float(record["lastPrice"])
        float(record["volume"])
    except (ValueError, TypeError) as e:
        return False, f"Non-numeric price/volume: {e}"
    return True, ""


# ── Wait for Schema Registry ──────────────────────────────────────────────────
def wait_for_schema_registry(url: str, retries: int = 20, delay: int = 5):
    import requests
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f"{url}/subjects", timeout=5)
            if resp.status_code == 200:
                log.info("✅ Schema Registry ready at %s", url)
                return
        except Exception:
            pass
        log.warning("Schema Registry not ready (attempt %d/%d). Retrying…", attempt, retries, delay)
        time.sleep(delay)
    raise RuntimeError("Schema Registry unavailable.")


# ── Connections ───────────────────────────────────────────────────────────────
def create_pg_connection(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )
            conn.autocommit = False
            log.info("✅ Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("Postgres not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL.")


def log_error(cursor, conn, error_type, error_message, raw, topic, partition, offset):
    try:
        cursor.execute(INSERT_ERROR, (
            "consumer", error_type, error_message,
            raw, topic, partition, offset,
        ))
        conn.commit()
        log.warning("⚠️  DLQ: [%s] %s", error_type, error_message)
    except Exception as e:
        conn.rollback()
        log.error("Failed to log error: %s", e)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Wait for Schema Registry
    wait_for_schema_registry(SCHEMA_REGISTRY_URL)

    # Set up Avro deserializer
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    # Set up Confluent Kafka consumer
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           KAFKA_GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("✅ Consumer subscribed to '%s' with Schema Registry validation", KAFKA_TOPIC)

    conn   = create_pg_connection()
    cursor = conn.cursor()

    good_batch  = []
    error_count = 0
    total_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush any remaining batch after idle
                if good_batch:
                    try:
                        execute_batch(cursor, INSERT_BRONZE, good_batch)
                        conn.commit()
                        log.info("🥉 Bronze: flushed %d records (idle flush)", len(good_batch))
                    except Exception as e:
                        conn.rollback()
                        log.error("Idle flush failed: %s", e)
                    finally:
                        good_batch.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            total_count += 1

            # ── Step 1: Avro deserialization (schema validation happens here) ──
            try:
                record = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
                if record is None:
                    raise ValueError("Deserialized record is None")
            except Exception as e:
                log_error(cursor, conn, "avro_deserialization_error", str(e),
                          str(msg.value()), msg.topic(), msg.partition(), msg.offset())
                error_count += 1
                continue

            # ── Step 2: Business validation ───────────────────────────────────
            is_valid, reason = validate_record(record)
            if not is_valid:
                log_error(cursor, conn, "validation_error", reason,
                          json.dumps(record), msg.topic(), msg.partition(), msg.offset())
                error_count += 1
                continue

            # ── Step 3: Add to batch ──────────────────────────────────────────
            good_batch.append(record)

            # ── Step 4: Flush batch to Bronze ─────────────────────────────────
            if len(good_batch) >= BATCH_SIZE:
                try:
                    execute_batch(cursor, INSERT_BRONZE, good_batch)
                    conn.commit()
                    log.info(
                        "🥉 Bronze: inserted %d records | ⚠️  schema errors: %d / %d total",
                        len(good_batch), error_count, total_count
                    )
                except Exception as e:
                    conn.rollback()
                    log.error("Batch insert failed: %s — logging to error_log", e)
                    for rec in good_batch:
                        log_error(cursor, conn, "db_insert_error", str(e),
                                  json.dumps(rec), msg.topic(), msg.partition(), msg.offset())
                    error_count += len(good_batch)
                finally:
                    good_batch.clear()

    except KeyboardInterrupt:
        log.info("Consumer shutting down…")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
