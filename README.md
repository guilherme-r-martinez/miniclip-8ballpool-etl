# 8 Ball Pool Data Engineering POC (Bronze / Silver / Gold)

Built by **Guilherme Martinez**.

## Purpose

The purpose of this project is to demonstrate an end-to-end, modular data engineering pipeline for mobile gaming telemetry—specifically simulating *8 Ball Pool* player session (`init`), gameplay (`match`), and monetization (`in-app-purchase`) events across a **Bronze / Silver / Gold** architecture. It is designed to be self-contained on a local Windows workstation without requiring administrator privileges, Docker, or local JVM/Hadoop file writers, while keeping a PySpark-ready structure for distributed environments.

## Architecture Overview

This POC provides two execution paths that produce the same Silver and Gold outputs:

## Sample Data & Outputs

Sample generated Bronze, Silver, and Gold files are included in the repository so reviewers can inspect the event schemas, Data Quality enrichments, and aggregated metrics (`daily_users.json` and `minute_kpis.json`) right away without having to run the pipeline first.

1. **Direct Local Python Path**: Runs event generation, DQ processing, and Gold aggregations directly through standard Python modules (`src/dq/processor.py` and `src/gold/run_gold.py`).
2. **Kafka Producer / Consumer Path**: Streams events via `src/producer/kafka_producer.py` to a Kafka topic (auto-created by the broker when enabled) and consumes them in `src/consumer/kafka_consumer.py` to apply DQ rules, write Silver JSONL, and refresh Gold outputs—with local JSONL/dry-run options when no broker is running.

Key components:

* **Event Generator (`src/producer/event_generator.py`)**: Produces deterministic synthetic `init`, `match`, and `in-app-purchase` events.
* **Kafka Producer (`src/producer/kafka_producer.py`)**: Publishes events to a Kafka topic (or writes a local dry-run JSONL file via `--dry-run-jsonl`).
* **Kafka Consumer (`src/consumer/kafka_consumer.py`)**: Consumes topic messages (or reads `--input-jsonl`), applies DQ rules via `process_stream`, appends to Silver, and updates Gold outputs.
* **Bronze (`data/bronze/`)**: Raw simulated JSONL events prior to DQ standardization.
* **Silver (`data/silver/`)**: DQ-cleaned and enriched JSONL events driven by `config/dq_rules.yaml` and `src/dq/rules.py`.
* **Gold (`data/gold/`)**: Aggregated analytics (`daily_users.json` and `minute_kpis.json`) generated via `src/gold/run_gold.py` or `src/consumer/kafka_consumer.py`.
* **PySpark Reference Modules (`src/spark/`)**: Batch (`batch_daily_users.py`), streaming (`stream_minute_kpis.py`), session builder (`session.py`), and shared DataFrame transformations (`transformations.py`) for Spark environments.

## Event Schemas

1. **`init`**
   * Captures initial session/player context including `event-type` (`"init"`), `user-id`, `country` (ISO code), `platform`, and `time` (epoch timestamp).
   * Tracks player country metadata so downstream events can inherit country context when needed.
2. **`match`**
   * Represents completed matches with `event-type` (`"match"`), `user-a`, `user-b`, `time`, and nested post-match metadata (`user-a-postmatch-info`, `user-b-postmatch-info`, including platform details).
3. **`in-app-purchase`**
   * Represents store transactions with `event-type` (`"in-app-purchase"`), `user-id`, `purchase_value` (or transaction amount), and `time`.

## Data Quality Rules (`config/dq_rules.yaml`)

Rules are declared in YAML and executed by `RuleEngine` (`src/dq/rules.py`) via `process_stream` (`src/dq/processor.py`):

* **Uppercase Normalization (`uppercase`)**: Standardizes platform fields (top-level `platform` and nested post-match platform fields) to uppercase.
* **Country Lookup Mapping (`map_lookup`)**: Maps ISO country codes (such as `PT`, `ES`, `GB`, `US`, `BR`, `FR`, `DE`) to human-readable `country_name` values, falling back to a configured default (`UNKNOWN`) when unmapped.
* **Session / User Country Enrichment**: Propagates known user country context from `init` events to subsequent user events when missing.

## Gold Metrics

* **Daily Users (`daily_users.json`)**: Distinct active users grouped by `event_date`, `country_name`, and `platform_clean`.
* **1-Minute KPIs (`minute_kpis.json`)**: Per-minute and per-country metrics including `purchase_count`, `total_revenue`, `distinct_users`, `country_revenue`, and `match_count`.

## How to Run Locally

From the project root directory with your virtual environment active:

### 1. Direct Local Python Path

```powershell
# Generate Bronze & DQ-cleaned Silver JSONL
py -m src.dq.processor

# Compute Gold daily_users.json and minute_kpis.json (zero-JVM Windows path)
py -m src.gold.run_gold

### 2. Kafka Producer (`src/producer/kafka_producer.py`)

A reviewer can run the producer locally either in offline dry-run mode (no Kafka broker required) or against a live Kafka broker:

```powershell
# 1. Offline local dry-run (writes JSONL without a running broker)
py -m src.producer.kafka_producer --dry-run-jsonl data/bronze/kafka_dry_run.jsonl --max-events 30

# 2. Publish to a running Kafka broker
py -m src.producer.kafka_producer --bootstrap-servers localhost:9092 --topic 8ballpool-events --max-events 80
