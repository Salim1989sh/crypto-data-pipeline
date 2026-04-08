import os
import json
import time
import logging
import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC", "crypto-ticker")
SCHEMA_REGISTRY_URL      = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
BINANCE_API_URL          = os.getenv("BINANCE_API_URL", "https://api.binance.com/api/v3/ticker/24hr")
FETCH_INTERVAL           = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
JSON_OUTPUT_PATH         = os.getenv("JSON_OUTPUT_PATH", "/data/latest_tickers.json")

# ── Avro Schema ───────────────────────────────────────────────────────────────
AVRO_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "CryptoTicker",
    "namespace": "com.crypto.pipeline",
    "fields": [
        {"name": "symbol",             "type": "string"},
        {"name": "priceChange",        "type": ["null", "string"], "default": None},
        {"name": "priceChangePercent", "type": ["null", "string"], "default": None},
        {"name": "weightedAvgPrice",   "type": ["null", "string"], "default": None},
        {"name": "prevClosePrice",     "type": ["null", "string"], "default": None},
        {"name": "lastPrice",          "type": "string"},
        {"name": "lastQty",            "type": ["null", "string"], "default": None},
        {"name": "bidPrice",           "type": ["null", "string"], "default": None},
        {"name": "askPrice",           "type": ["null", "string"], "default": None},
        {"name": "openPrice",          "type": ["null", "string"], "default": None},
        {"name": "highPrice",          "type": ["null", "string"], "default": None},
        {"name": "lowPrice",           "type": ["null", "string"], "default": None},
        {"name": "volume",             "type": "string"},
        {"name": "quoteVolume",        "type": ["null", "string"], "default": None},
        {"name": "openTime",           "type": ["null", "long"],   "default": None},
        {"name": "closeTime",          "type": ["null", "long"],   "default": None},
        {"name": "count",              "type": ["null", "long"],   "default": None}
    ]
})


# ── Wait for Schema Registry ──────────────────────────────────────────────────
def wait_for_schema_registry(url: str, retries: int = 20, delay: int = 5):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f"{url}/subjects", timeout=5)
            if resp.status_code == 200:
                log.info("✅ Schema Registry ready at %s", url)
                return
        except Exception:
            pass
        log.warning("Schema Registry not ready (attempt %d/%d). Retrying in %ds…", attempt, retries, delay)
        time.sleep(delay)
    raise RuntimeError("Schema Registry not available after multiple retries.")


# ── Map Binance API response to Avro record ───────────────────────────────────
def to_avro_record(ticker: dict) -> dict:
    """Map Binance ticker fields to our Avro schema fields."""
    return {
        "symbol":             ticker.get("symbol", ""),
        "priceChange":        ticker.get("priceChange"),
        "priceChangePercent": ticker.get("priceChangePercent"),
        "weightedAvgPrice":   ticker.get("weightedAvgPrice"),
        "prevClosePrice":     ticker.get("prevClosePrice"),
        "lastPrice":          ticker.get("lastPrice", "0"),
        "lastQty":            ticker.get("lastQty"),
        "bidPrice":           ticker.get("bidPrice"),
        "askPrice":           ticker.get("askPrice"),
        "openPrice":          ticker.get("openPrice"),
        "highPrice":          ticker.get("highPrice"),
        "lowPrice":           ticker.get("lowPrice"),
        "volume":             ticker.get("volume", "0"),
        "quoteVolume":        ticker.get("quoteVolume"),
        "openTime":           ticker.get("openTime"),
        "closeTime":          ticker.get("closeTime"),
        "count":              ticker.get("count"),
    }


def fetch_tickers() -> list[dict]:
    response = requests.get(BINANCE_API_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def save_to_json(tickers: list[dict]) -> None:
    os.makedirs(os.path.dirname(JSON_OUTPUT_PATH), exist_ok=True)
    payload = {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": len(tickers),
        "tickers": tickers,
    }
    tmp = JSON_OUTPUT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, JSON_OUTPUT_PATH)
    log.info("Saved %d tickers to %s", len(tickers), JSON_OUTPUT_PATH)


def delivery_report(err, msg):
    if err:
        log.error("❌ Delivery failed for %s: %s", msg.key(), err)


def main():
    # ── Wait for Schema Registry ──────────────────────────────────────────────
    wait_for_schema_registry(SCHEMA_REGISTRY_URL)

    # ── Set up Schema Registry client and Avro serializer ────────────────────
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client,
        AVRO_SCHEMA_STR,
        lambda obj, ctx: obj,   # our record is already a plain dict
    )

    # ── Set up Confluent Kafka producer ───────────────────────────────────────
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    log.info("✅ Producer connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("📋 Schema registered at %s for topic '%s'", SCHEMA_REGISTRY_URL, KAFKA_TOPIC)

    log.info("Starting producer loop — fetching every %d seconds", FETCH_INTERVAL)

    while True:
        try:
            tickers = fetch_tickers()
            log.info("Fetched %d tickers from Binance", len(tickers))

            # Save raw JSON to file
            save_to_json(tickers)

            # Serialize and send each ticker as Avro
            sent = 0
            errors = 0
            for ticker in tickers:
                try:
                    record = to_avro_record(ticker)
                    serialized = avro_serializer(
                        record,
                        SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
                    )
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        value=serialized,
                        key=record["symbol"].encode("utf-8"),
                        on_delivery=delivery_report,
                    )
                    sent += 1
                except Exception as e:
                    log.error("Schema validation failed for %s: %s", ticker.get("symbol"), e)
                    errors += 1

            producer.flush()
            log.info("✅ Sent %d messages | ❌ Schema rejected: %d", sent, errors)

        except requests.RequestException as e:
            log.error("HTTP error fetching tickers: %s", e)
        except Exception as e:
            log.error("Unexpected error: %s", e)

        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
