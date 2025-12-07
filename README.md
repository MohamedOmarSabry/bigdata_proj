# GlobalMart Real-Time Data Pipeline

End-to-end streaming + batch analytics stack for GlobalMart:
- **Data generation** → Kafka topics
- **Stream processing** (Spark Structured Streaming) → staging Parquet + real-time metrics/alerts
- **Batch ETL** → aggregates to warehouse
- **Data Explorer UI & API server** → browse/query curated data

---

## 1) Prerequisites

- Python 3.10+
- Java 11+
- Apache Kafka
- Apache Spark (PySpark via pip)
- PostgreSQL, Grafana and Prometheus

---

## 2) Setup & Installation

```bash
# Clone and enter project
git clone https://github.com/MohamedOmarSabry/bigdata_proj.git
cd bigdata_proj

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python deps (Spark, Kafka client, web server, etc.)
pip install -r requirements.txt
```

Ensure Kafka is up and topics exist:
```bash
# Start Kafka locally (adjust to your install)
# e.g., if using local Kafka install
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties

# Create topics (idempotent)
for t in globalmart.user_events globalmart.product_events globalmart.view_events globalmart.cart_events globalmart.transaction_events; do
  kafka-topics --create --if-not-exists --topic $t --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
done
```

---

## 3) Configuration Guide

Edit `config.py` to set:
- **KAFKA_CONFIG**: `bootstrap_servers`, topic names
- **PATHS**: staging Parquet paths, checkpoints
- **SPARK_CONFIG**: executor counts, memory, shuffle partitions
- **ANOMALY_DETECTION**: thresholds (cart abandonment timeout/value, inventory levels)
- **API / Web**: host/port if applicable

Example key settings (already present):
- `spark.executor.instances`, `spark.executor.cores`, `spark.executor.memory`
- `spark.sql.shuffle.partitions` (lower for local: 8–32)
- Checkpoint dirs under `Staging/checkpoints/`

---

## 4) How to Run

### A) Generate Synthetic Data → Kafka
```bash
source venv/bin/activate
python generate_events.py \
  --rate 500 \             # events per second total
  --duration 600 \         # seconds
  --bootstrap localhost:9092
```

### B) Run Stream Processing (Spark Structured Streaming)
Consumes Kafka, validates, stages Parquet, emits metrics/alerts.
```bash
source venv/bin/activate
python stream_processing.py
```
Pipelines:
- Validation & staging (clean/quarantine)
- Real-time metrics (sales by hour/category/country)
- Anomaly detection (transactions, inventory, cart abandonment)
- Session analysis

Outputs:
- Parquet under `data/staging/clean/` and `data/staging/quarantine/`
- Metrics/alerts to Kafka metrics topics
- Checkpoints under `Staging/checkpoints/`

### C) Real-Time Metrics Dashboard
```bash
source venv/bin/activate
python dashboard.py
```

### D) Run Batch ETL (warehouse aggregates)
```bash
source venv/bin/activate
python batch_processing.py
```
Reads staged Parquet, computes daily/weekly aggregates, and (optionally) writes to warehouse/DB.

### E) Data Explorer Web Interface
```bash
source venv/bin/activate
python api_server.py
```
Provides REST endpoints over curated datasets/metrics.

---

## 6) Troubleshooting (Common Issues)

- **Kafka connection refused**  
  Verify broker up: `kafka-topics --list --bootstrap-server localhost:9092`. Start Kafka, then rerun.

- **Too many small Parquet files / slow reads**  
  Tune `spark.sql.shuffle.partitions` lower for local (8–32) and optionally `coalesce()` before writes.

- **OutOfMemory / GC pressure**  
  Increase `spark.executor.memory`, reduce `maxOffsetsPerTrigger`, add watermarking on event-time columns.

- **Late data not counted / state blow-up**  
  Add `.withWatermark("timestamp", "10 minutes")` to aggregations; adjust window sizes.

- **Dashboard not loading**  
  Confirm `dashboard.py` is running and Kafka metrics topics are populated; check terminal logs for port conflicts.

- **API 500 errors**  
  Check database/Parquet paths in `config.py`; validate that batch ETL has produced curated tables.

---

## 7) Project Structure (key files)

```
stream_processing.py      # Streaming pipelines (validation, metrics, anomalies, sessions)
batch_processing.py       # Batch ETL / warehouse aggregates
data_quality.py           # Validation & quarantine rules
session_analysis.py       # Session lifecycle & analytics
real_time_analytics.py    # Metrics aggregation helpers
dashboard.py              # Real-time dashboard (web UI)
api_server.py             # REST API for curated data/metrics
generate_events.py        # Synthetic data → Kafka
config.py                 # All config (Spark, Kafka, paths, thresholds)
data_listener.py          # Consumer of Kafka streamers to monitor data generator
```

---