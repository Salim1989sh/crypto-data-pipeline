import os
import json
import time
import logging
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DLQ-CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_DLQ_TOPIC         = os.getenv("KAFKA_DLQ_TOPIC", "crypto-ticker-dlq")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "crypto-dlq-group")
PG_HOST                 = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT                 = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB                   = os.getenv("POSTGRES_DB", "crypto_db")
PG_USER                 = os.getenv("POSTGRES_USER", "crypto_user")
PG_PASSWORD             = os.getenv("POSTGRES_PASSWORD", "crypto_pass")

INSERT_DLQ_LOG = """
INSERT INTO bronze.error_log (
    source, error_type, error_message, raw_payload,
    kafka_topic, kafka_partition, kafka_offset
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


def create_consumer(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_DLQ_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            log.info("DLQ consumer connected to '%s'", KAFKA_DLQ_TOPIC)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/%d). Retrying…", attempt, retries)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka DLQ.")


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


def main():
    consumer = create_consumer()
    conn     = create_pg_connection()
    cursor   = conn.cursor()

    log.info("DLQ consumer running — monitoring dead letter queue")

    for message in consumer:
        try:
            record = message.value
            log.warning(
                "💀 DLQ message received | error_type=%s | topic=%s | offset=%d | reason=%s",
                record.get("error_type", "unknown"),
                record.get("original_topic", "unknown"),
                record.get("offset", -1),
                record.get("error_message", "unknown"),
            )

            cursor.execute(INSERT_DLQ_LOG, (
                "dlq_consumer",
                record.get("error_type", "unknown"),
                record.get("error_message", "unknown"),
                json.dumps(record.get("payload")),
                record.get("original_topic"),
                message.partition,
                message.offset,
            ))
            conn.commit()

        except Exception as e:
            conn.rollback()
            log.error("Failed to process DLQ message: %s", e)


if __name__ == "__main__":
    main()
