================================================================================
          CRYPTO DATA PIPELINE — COMPLETE SETUP & USAGE GUIDE
================================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 1. PROJECT OVERVIEW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This project is a production-grade real-time data pipeline that fetches live
cryptocurrency price data from the Binance API every 60 seconds, processes it
through a Medallion Architecture (Bronze → Silver → Gold), and visualizes the
results in a Grafana dashboard. The entire pipeline runs inside 11 Docker
containers on WSL2.

ARCHITECTURE SUMMARY
┌─────────────────────┬──────────────────────────────────────────────────────┐
│ Component           │ Purpose                                               │
├─────────────────────┼──────────────────────────────────────────────────────┤
│ Binance API         │ Live source — 3,559 crypto tickers every 60 seconds  │
│ Producer            │ Fetches data, serializes as Avro, sends to Kafka     │
│ Schema Registry     │ Validates every Kafka message against Avro contract  │
│ Kafka               │ Message broker — topics: crypto-ticker, DLQ topic   │
│ Consumer            │ Reads from Kafka, writes raw data to Bronze layer    │
│ DLQ Consumer        │ Captures failed/invalid messages to error log        │
│ Spark Streaming     │ 5-minute aggregations directly into Gold layer       │
│ Transformer         │ Bronze → Silver → Gold SQL transformations           │
│ Airflow             │ Orchestrates the full DAG every 5 minutes            │
│ PostgreSQL          │ Stores all 3 medallion layers                        │
│ Grafana             │ Real-time dashboard — reads from Gold layer          │
└─────────────────────┴──────────────────────────────────────────────────────┘

PIPELINE FLOW
  Binance API
      │
      ▼
  Producer ──────────────────────────────────► JSON file (data/latest_tickers.json)
      │
      ▼ (Avro serialization)
  Schema Registry (validates schema)
      │
      ▼
  Kafka (topic: crypto-ticker)
      │
      ├──► Consumer ──────────────────► 🥉 Bronze (raw TEXT data)
      │                                       │
      ├──► DLQ Consumer ─────────────► bronze.error_log
      │
      └──► Spark Streaming ──────────► 🥇 Gold (spark_aggregations)
                                       │
  Transformer (every 60s)              │
      ├──► Bronze → 🥈 Silver          │
      └──► Silver → 🥇 Gold ◄──────────┘

  Airflow (every 5 min) ──► orchestrates Bronze → Silver → Gold
  Grafana ──────────────────► reads from Gold → dashboard


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 2. PREREQUISITES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Install the following on your Windows machine before starting:

  • Windows 10/11 with WSL2 enabled
  • Docker Desktop with WSL2 backend enabled
  • Ubuntu or Debian installed inside WSL2
  • Minimum 8 GB RAM allocated to Docker
  • At least 20 GB free disk space

TIP: In Docker Desktop → Settings → Resources → WSL Integration,
     make sure your WSL2 distro is toggled ON.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 3. PROJECT FOLDER STRUCTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

All files must be placed in this exact structure:

  D:\Projects\crypto-data-pipeline\docker\

  docker/
  ├── docker-compose.yml
  ├── init.sql
  ├── init_airflow.sql
  ├── producer/
  │   ├── Dockerfile
  │   ├── requirements.txt
  │   ├── producer.py
  │   └── config.py
  ├── consumer/
  │   ├── Dockerfile
  │   ├── requirements.txt
  │   └── consumer.py
  ├── dlq_consumer/
  │   ├── Dockerfile
  │   ├── requirements.txt
  │   └── dlq_consumer.py
  ├── transformer/
  │   ├── Dockerfile
  │   ├── requirements.txt
  │   └── transformer.py
  ├── spark/
  │   ├── Dockerfile
  │   └── spark_streaming.py
  ├── airflow/
  │   ├── dags/
  │   │   └── crypto_pipeline.py
  │   └── logs/
  ├── schema/
  │   └── crypto_ticker.avsc
  ├── data/
  │   └── latest_tickers.json   ← auto-created by producer
  └── grafana/
      └── provisioning/
          ├── datasources/
          │   └── postgres.yml
          └── dashboards/
              ├── dashboard.yml
              └── crypto_pipeline.json


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 4. STARTING THE PIPELINE — STEP BY STEP
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 1 — Open WSL2 terminal and navigate to the project folder
  cd /mnt/d/Projects/crypto-data-pipeline/docker

STEP 2 — Create required empty folders (run once only)
  mkdir -p airflow/dags airflow/logs data

STEP 3 — Build and start all 11 containers
  docker compose up --build -d

  NOTE: First startup downloads all Docker images — takes 3 to 5 minutes.
        Subsequent starts are much faster.

STEP 4 — Verify all containers are running
  docker ps

  Expected containers and status:
  ┌──────────────────────┬─────────────────────────┐
  │ Container            │ Expected Status          │
  ├──────────────────────┼─────────────────────────┤
  │ zookeeper            │ healthy                  │
  │ kafka                │ healthy                  │
  │ schema-registry      │ healthy                  │
  │ postgres             │ healthy                  │
  │ producer             │ Up                       │
  │ consumer             │ Up                       │
  │ dlq_consumer         │ Up                       │
  │ transformer          │ Up                       │
  │ spark                │ Up                       │
  │ airflow-webserver    │ Up (health: starting)    │
  │ airflow-scheduler    │ Up                       │
  │ grafana              │ Up                       │
  └──────────────────────┴─────────────────────────┘

STEP 5 — Verify data is flowing through all 3 layers
  docker exec -it postgres psql -U crypto_user -d crypto_db -c "
  SELECT 'bronze' AS layer, COUNT(*) FROM bronze.crypto_ticker_raw
  UNION ALL
  SELECT 'silver', COUNT(*) FROM silver.crypto_ticker
  UNION ALL
  SELECT 'gold',   COUNT(*) FROM gold.latest_prices;"

  You should see all 3 rows with counts greater than 0.

STEP 6 — Watch live logs (optional)
  docker compose logs -f producer      ← watch data being fetched
  docker compose logs -f consumer      ← watch Bronze inserts
  docker compose logs -f transformer   ← watch Bronze→Silver→Gold
  docker compose logs -f spark         ← watch Spark aggregations


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 5. ACCESSING THE USER INTERFACES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌─────────────────────┬─────────────────────────────┬────────────────────────┐
│ Service             │ URL                          │ Credentials            │
├─────────────────────┼─────────────────────────────┼────────────────────────┤
│ Grafana Dashboard   │ http://localhost:3000         │ admin / admin          │
│ Airflow UI          │ http://localhost:8080         │ admin / admin          │
│ Schema Registry     │ http://localhost:8081         │ no login required      │
│ PostgreSQL          │ localhost:5432                │ crypto_user/crypto_pass│
└─────────────────────┴─────────────────────────────┴────────────────────────┘

── GRAFANA ──────────────────────────────────────────────────────────────────

  1. Open http://localhost:3000
  2. Login: admin / admin
  3. Go to Dashboards → Crypto Pipeline — Medallion Architecture

  ADDING THE DATASOURCE (first time only — if dashboard shows No data):
  1. Go to Connections → Data sources → Add new data source
  2. Choose PostgreSQL
  3. Fill in EXACTLY:
       Name:          CryptoDB          ← MUST be exactly this, case sensitive
       Host URL:      postgres:5432
       Database name: crypto_db
       Username:      crypto_user
       Password:      crypto_pass
       TLS/SSL Mode:  disable
  4. Click Save & Test → should show "Database Connection OK"

  DASHBOARD PANELS:
  • 🥉 Bronze Raw Rows       — total records from Binance
  • 🥈 Silver Clean Rows     — after type casting and cleaning
  • 🥇 Gold Tracked Symbols  — unique coins tracked (3,559)
  • Last Gold Refresh        — timestamp of last Gold update
  • Bronze vs Silver chart   — ingestion rate over time
  • Top 20 Gainers           — biggest 24h % price increase
  • Top 20 Losers            — biggest 24h % price drop
  • Volume Leaders table     — ranked by trading volume

── AIRFLOW ──────────────────────────────────────────────────────────────────

  1. Open http://localhost:8080
  2. Login: admin / admin
  3. Find the DAG named: crypto_pipeline
  4. Click the play ▶ button to run manually, or wait for the automatic
     5-minute schedule

  READING THE DAG:
  • Green box  = task succeeded
  • Red box    = task failed → click it → click Log to see the error
  • Yellow box = task is currently running
  • Grey box   = task has not run yet

  To see task details: click any box → click Log


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 6. MEDALLION ARCHITECTURE EXPLAINED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🥉 BRONZE LAYER — Raw Data
  Everything stored exactly as received from Binance. No type casting,
  no cleaning. All numeric values stored as TEXT to preserve the original data.
  Table:   bronze.crypto_ticker_raw
  Updated: every 60 seconds

🥈 SILVER LAYER — Cleaned Data
  Bronze data after validation and type casting.
  - String prices cast to NUMERIC(20,8)
  - Timestamps converted from epoch milliseconds to TIMESTAMP WITH TIME ZONE
  - Records missing symbol, lastPrice, or volume are rejected to DLQ
  - Watermark tracking prevents double-processing
  Table:   silver.crypto_ticker
  Updated: every 60 seconds (via transformer) + every 30s (via Spark)

🥇 GOLD LAYER — Business Ready
  Aggregated and summarized data ready for dashboards and analysis.
  Tables:
    gold.latest_prices       → most recent price for each symbol
    gold.top_gainers         → top 20 symbols by 24h % gain
    gold.top_losers          → top 20 symbols by 24h % loss
    gold.volume_leaders      → top 20 by trading volume
    gold.hourly_summary      → OHLCV data per hour per symbol
    gold.spark_aggregations  → 5-minute avg/max/min from Spark Streaming
    gold.pipeline_health     → health snapshot written by Airflow


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 7. ERROR HANDLING & DEAD LETTER QUEUE (DLQ)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Every message that fails validation is routed to the Dead Letter Queue (DLQ)
Kafka topic instead of crashing the consumer. Errors are also written to
the bronze.error_log table in PostgreSQL.

WHAT GETS REJECTED TO DLQ:
  • Avro schema validation failure (wrong field types or missing fields)
  • Missing required fields: symbol, lastPrice, or volume
  • Non-numeric values in price or volume fields
  • Full batch database insert failures

CHECKING THE ERROR LOG:
  docker exec -it postgres psql -U crypto_user -d crypto_db -c \
  "SELECT error_type, COUNT(*) FROM bronze.error_log GROUP BY error_type;"

WATCHING THE DLQ CONSUMER:
  docker compose logs -f dlq_consumer


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 8. AIRFLOW DAG — crypto_pipeline
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

The DAG runs every 5 minutes and executes these tasks in order:

  start
    ├── check_kafka_health          → verify Kafka broker is reachable
    └── check_postgres_health       → confirm all 3 schemas exist
          └── validate_bronze_ingestion   → check new rows arrived (last 10 min)
                └── run_bronze_to_silver  → Bronze → Silver SQL transformation
                      └── validate_silver_data   → check for null symbols/prices
                            └── run_silver_to_gold  → refresh all Gold tables
                                  └── validate_gold_data  → confirm Gold is filled
                                        ├── update_pipeline_health
                                        └── alert_on_errors
                                              └── end

TIP: If a task turns red, click it in the Airflow graph then click Log
     to see the exact Python error message and fix it.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 9. SCHEMA REGISTRY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

The Schema Registry enforces an Avro schema contract on every Kafka message.
The producer registers the schema automatically on startup. Messages that do
not match the schema are rejected before reaching the consumer.

VERIFY THE SCHEMA IS REGISTERED:
  curl http://localhost:8081/subjects
  curl http://localhost:8081/subjects/crypto-ticker-value/versions/latest

AVRO SCHEMA FIELDS:
  ┌──────────────────────┬───────────────────────────┐
  │ Field                │ Type                      │
  ├──────────────────────┼───────────────────────────┤
  │ symbol               │ string (required)         │
  │ lastPrice            │ string (required)         │
  │ volume               │ string (required)         │
  │ priceChange          │ string or null            │
  │ priceChangePercent   │ string or null            │
  │ highPrice            │ string or null            │
  │ lowPrice             │ string or null            │
  │ openTime             │ long or null              │
  │ closeTime            │ long or null              │
  │ count                │ long or null              │
  └──────────────────────┴───────────────────────────┘


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 10. SPARK STRUCTURED STREAMING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Spark runs as a separate container and reads directly from Kafka.
It runs two parallel streams:

STREAM 1 — Silver writer (every 30 seconds)
  Kafka → parse JSON → cast field types → filter nulls → silver.crypto_ticker

STREAM 2 — Gold aggregations (every 30 seconds)
  Kafka → 5-minute window by symbol → avg/max/min price, total volume
        → gold.spark_aggregations

VERIFY SPARK IS WORKING:
  docker logs spark --tail 30

QUERY SPARK GOLD DATA:
  docker exec -it postgres psql -U crypto_user -d crypto_db -c \
  "SELECT symbol, avg_price, max_price, min_price, total_volume,
          window_start, window_end
   FROM gold.spark_aggregations
   ORDER BY computed_at DESC
   LIMIT 10;"


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 11. STOPPING THE PIPELINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Stop all containers — keep all PostgreSQL data and Grafana settings:
  docker compose down

Full reset — delete ALL data and volumes (cannot be undone):
  docker compose down -v

  WARNING: Using -v permanently deletes all PostgreSQL data,
           Grafana settings, and Airflow state.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 12. TROUBLESHOOTING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌────────────────────────────────┬───────────────────────────────────────────┐
│ Problem                        │ Solution                                  │
├────────────────────────────────┼───────────────────────────────────────────┤
│ Grafana shows No data          │ Check datasource Name is exactly           │
│                                │ 'CryptoDB' (case sensitive)               │
├────────────────────────────────┼───────────────────────────────────────────┤
│ Kafka container exits          │ docker logs zookeeper --tail 20           │
│                                │ Zookeeper must start and be healthy first │
├────────────────────────────────┼───────────────────────────────────────────┤
│ Grafana login fails            │ docker exec -it grafana grafana-cli       │
│                                │ admin reset-admin-password admin          │
├────────────────────────────────┼───────────────────────────────────────────┤
│ Producer cannot reach Kafka    │ Kafka takes 20-30s to start.              │
│                                │ Producer retries automatically. Wait 1 min│
├────────────────────────────────┼───────────────────────────────────────────┤
│ Airflow permission denied      │ docker exec -it postgres psql             │
│                                │ -U crypto_user -d airflow_db -c          │
│                                │ 'GRANT ALL ON SCHEMA public              │
│                                │  TO airflow_user;'                       │
├────────────────────────────────┼───────────────────────────────────────────┤
│ Docker credential error        │ Edit ~/.docker/config.json                │
│                                │ Replace ALL content with: {}              │
│                                │ Then retry docker compose up             │
├────────────────────────────────┼───────────────────────────────────────────┤
│ Port already in use            │ Change host port in docker-compose.yml   │
│                                │ e.g. "3001:3000" for Grafana             │
└────────────────────────────────┴───────────────────────────────────────────┘


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 13. USEFUL COMMANDS REFERENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

── Monitor logs ─────────────────────────────────────────────────────────────

  docker compose logs -f producer      # watch live data fetches
  docker compose logs -f consumer      # watch Bronze inserts
  docker compose logs -f transformer   # watch Bronze→Silver→Gold
  docker compose logs -f spark         # watch Spark aggregations
  docker compose logs -f airflow-scheduler  # watch Airflow runs

── Database queries ──────────────────────────────────────────────────────────

  # Row counts across all 3 layers
  docker exec -it postgres psql -U crypto_user -d crypto_db -c "
  SELECT 'bronze' AS layer, COUNT(*) FROM bronze.crypto_ticker_raw
  UNION ALL SELECT 'silver', COUNT(*) FROM silver.crypto_ticker
  UNION ALL SELECT 'gold',   COUNT(*) FROM gold.latest_prices;"

  # Top 10 gainers
  docker exec -it postgres psql -U crypto_user -d crypto_db -c \
  "SELECT symbol, price_change_pct, last_price
   FROM gold.top_gainers ORDER BY rank LIMIT 10;"

  # Top 10 by volume
  docker exec -it postgres psql -U crypto_user -d crypto_db -c \
  "SELECT symbol, quote_volume, last_price
   FROM gold.volume_leaders ORDER BY rank LIMIT 10;"

  # Error log summary
  docker exec -it postgres psql -U crypto_user -d crypto_db -c \
  "SELECT error_type, COUNT(*) FROM bronze.error_log GROUP BY error_type;"

  # Spark aggregations
  docker exec -it postgres psql -U crypto_user -d crypto_db -c \
  "SELECT symbol, avg_price, max_price, total_volume
   FROM gold.spark_aggregations ORDER BY computed_at DESC LIMIT 10;"

── Kafka commands ────────────────────────────────────────────────────────────

  # List all topics
  docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

  # View 5 messages from the topic
  docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic crypto-ticker --from-beginning --max-messages 5

  # Check DLQ messages
  docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic crypto-ticker-dlq --from-beginning --max-messages 5

── Schema Registry ───────────────────────────────────────────────────────────

  # List registered schemas
  curl http://localhost:8081/subjects

  # View the crypto-ticker schema
  curl http://localhost:8081/subjects/crypto-ticker-value/versions/latest

── Container management ──────────────────────────────────────────────────────

  docker ps                        # list running containers
  docker compose restart producer  # restart a single service
  docker stats                     # live CPU/memory usage per container


================================================================================

