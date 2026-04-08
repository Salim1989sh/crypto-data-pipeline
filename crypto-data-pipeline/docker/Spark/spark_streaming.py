"""
Spark Structured Streaming — Crypto Pipeline
============================================
Reads from Kafka topic 'crypto-ticker' (JSON messages)
Transforms and writes to PostgreSQL Silver + Gold layers

Streams:
  1. Bronze → Silver  : parse, cast, clean fields
  2. Silver → Gold    : aggregations per symbol (avg, max, min price)

Schedule: continuous micro-batch, trigger every 30 seconds
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "crypto-ticker")
PG_HOST                 = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT                 = os.getenv("POSTGRES_PORT", "5432")
PG_DB                   = os.getenv("POSTGRES_DB", "crypto_db")
PG_USER                 = os.getenv("POSTGRES_USER", "crypto_user")
PG_PASSWORD             = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
TRIGGER_INTERVAL        = os.getenv("TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT_DIR          = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPERTIES = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Kafka JSON Schema ─────────────────────────────────────────────────────────
# Matches exactly what the producer sends
TICKER_SCHEMA = StructType([
    StructField("symbol",             StringType(), nullable=False),
    StructField("priceChange",        StringType(), nullable=True),
    StructField("priceChangePercent", StringType(), nullable=True),
    StructField("weightedAvgPrice",   StringType(), nullable=True),
    StructField("prevClosePrice",     StringType(), nullable=True),
    StructField("lastPrice",          StringType(), nullable=False),
    StructField("lastQty",            StringType(), nullable=True),
    StructField("bidPrice",           StringType(), nullable=True),
    StructField("askPrice",           StringType(), nullable=True),
    StructField("openPrice",          StringType(), nullable=True),
    StructField("highPrice",          StringType(), nullable=True),
    StructField("lowPrice",           StringType(), nullable=True),
    StructField("volume",             StringType(), nullable=False),
    StructField("quoteVolume",        StringType(), nullable=True),
    StructField("openTime",           LongType(),   nullable=True),
    StructField("closeTime",          LongType(),   nullable=True),
    StructField("count",              LongType(),   nullable=True),
])


# ── Spark Session ─────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    log.info("Creating Spark session…")
    spark = (
        SparkSession.builder
        .appName("CryptoPipelineStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Kafka connector
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("✅ Spark session created")
    return spark


# ── Read from Kafka ───────────────────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession):
    log.info("Connecting to Kafka topic '%s'…", KAFKA_TOPIC)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .load()
    )


# ── Bronze → Silver transformation ────────────────────────────────────────────
def transform_to_silver(raw_df):
    """
    Parse JSON from Kafka value, cast all string fields to proper types,
    filter out invalid records, add ingestion timestamp.
    """
    log.info("Setting up Bronze → Silver transformation…")

    # Parse JSON value from Kafka
    parsed = (
        raw_df
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition"),
            F.col("offset"),
            F.from_json(
                F.col("value").cast("string"),
                TICKER_SCHEMA
            ).alias("data")
        )
        .select("kafka_timestamp", "partition", "offset", "data.*")
    )

    # Cast string fields to proper numeric types
    silver = (
        parsed
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("lastPrice").isNotNull())
        .filter(F.col("volume").isNotNull())
        .withColumn("last_price",        F.col("lastPrice").cast(DoubleType()))
        .withColumn("price_change",      F.col("priceChange").cast(DoubleType()))
        .withColumn("price_change_pct",  F.col("priceChangePercent").cast(DoubleType()))
        .withColumn("weighted_avg_price",F.col("weightedAvgPrice").cast(DoubleType()))
        .withColumn("prev_close_price",  F.col("prevClosePrice").cast(DoubleType()))
        .withColumn("last_qty",          F.col("lastQty").cast(DoubleType()))
        .withColumn("bid_price",         F.col("bidPrice").cast(DoubleType()))
        .withColumn("ask_price",         F.col("askPrice").cast(DoubleType()))
        .withColumn("open_price",        F.col("openPrice").cast(DoubleType()))
        .withColumn("high_price",        F.col("highPrice").cast(DoubleType()))
        .withColumn("low_price",         F.col("lowPrice").cast(DoubleType()))
        .withColumn("volume",            F.col("volume").cast(DoubleType()))
        .withColumn("quote_volume",      F.col("quoteVolume").cast(DoubleType()))
        .withColumn("open_time",         (F.col("openTime") / 1000).cast(TimestampType()))
        .withColumn("close_time",        (F.col("closeTime") / 1000).cast(TimestampType()))
        .withColumn("trade_count",       F.col("count"))
        .withColumn("transformed_at",    F.current_timestamp())
        # Drop nulls in critical columns after casting
        .filter(F.col("last_price").isNotNull())
        .filter(F.col("volume").isNotNull())
        .select(
            "symbol", "price_change", "price_change_pct", "weighted_avg_price",
            "prev_close_price", "last_price", "last_qty", "bid_price", "ask_price",
            "open_price", "high_price", "low_price", "volume", "quote_volume",
            "open_time", "close_time", "trade_count", "transformed_at"
        )
    )

    return silver


# ── Silver → Gold aggregations ────────────────────────────────────────────────
def transform_to_gold(silver_df):
    """
    Compute aggregations per symbol over a 5-minute window:
    - avg, max, min last_price
    - total volume
    - avg price change %
    - record count
    """
    log.info("Setting up Silver → Gold aggregation…")

    gold = (
        silver_df
        .withWatermark("transformed_at", "10 minutes")
        .groupBy(
            F.window("transformed_at", "5 minutes").alias("window"),
            F.col("symbol")
        )
        .agg(
            F.avg("last_price").alias("avg_price"),
            F.max("high_price").alias("max_price"),
            F.min("low_price").alias("min_price"),
            F.sum("volume").alias("total_volume"),
            F.avg("price_change_pct").alias("avg_price_change_pct"),
            F.count("*").alias("record_count"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("avg_price"),
            F.col("max_price"),
            F.col("min_price"),
            F.col("total_volume"),
            F.col("avg_price_change_pct"),
            F.col("record_count"),
            F.current_timestamp().alias("computed_at"),
        )
    )

    return gold


# ── Write to PostgreSQL ───────────────────────────────────────────────────────
def write_to_postgres(df, epoch_id: int, table: str, mode: str = "append"):
    """Write a micro-batch DataFrame to PostgreSQL."""
    count = df.count()
    if count == 0:
        log.info("Epoch %d — no rows to write to %s", epoch_id, table)
        return
    try:
        (
            df.write
            .jdbc(
                url=PG_URL,
                table=table,
                mode=mode,
                properties=PG_PROPERTIES,
            )
        )
        log.info("✅ Epoch %d — wrote %d rows to %s", epoch_id, count, table)
    except Exception as e:
        log.error("❌ Epoch %d — failed to write to %s: %s", epoch_id, table, e)
        raise


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()

    # ── Stream 1: Kafka → Silver ──────────────────────────────────────────────
    raw_stream   = read_kafka_stream(spark)
    silver_df    = transform_to_silver(raw_stream)

    silver_query = (
        silver_df.writeStream
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver")
        .foreachBatch(lambda df, epoch: write_to_postgres(df, epoch, "silver.crypto_ticker"))
        .start()
    )
    log.info("✅ Silver stream started — writing to silver.crypto_ticker every %s", TRIGGER_INTERVAL)

    # ── Stream 2: Kafka → Gold (aggregations) ────────────────────────────────
    raw_stream2  = read_kafka_stream(spark)
    silver_df2   = transform_to_silver(raw_stream2)
    gold_df      = transform_to_gold(silver_df2)

    gold_query = (
        gold_df.writeStream
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold")
        .foreachBatch(lambda df, epoch: write_to_postgres(df, epoch, "gold.spark_aggregations"))
        .start()
    )
    log.info("✅ Gold stream started — writing to gold.spark_aggregations every %s", TRIGGER_INTERVAL)

    # ── Wait for termination ──────────────────────────────────────────────────
    log.info("Spark Streaming running — waiting for termination…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
