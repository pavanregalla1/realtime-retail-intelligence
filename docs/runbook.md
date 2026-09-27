# Runbook — exact commands

Every command runs from the repo root. The full sequence is automated in
`./run.sh`; this page documents each step individually for operators and
for anyone reproducing the results by hand.

Prereqs: Python 3.10+, `pip install -r requirements.txt`. Docker for the
Kafka steps. PowerShell users: replace `export X=y` with `$env:X="y"`.

---

## 1. Start Kafka

```bash
docker compose up -d
```

Wait until the broker is healthy (usually 15–30 s):

```bash
docker exec retail-kafka cub kafka-ready -b localhost:9092 1 5
```

Create the topics explicitly (they also auto-create on first publish):

```bash
docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic retail.events --partitions 3 --replication-factor 1
docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic retail.alerts --partitions 3 --replication-factor 1
docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Stop (data persists in the `kafka-data` volume):

```bash
docker compose down        # stop
docker compose down -v     # stop AND delete all topic data
```

## 2. Start the event producer

Local file mode (writes micro-batch files to `data/stream/` — the default,
used by the ML trainers and the file-replay Spark job):

```bash
python streaming/event_producer.py                 # 7 days, 336 batches
DAYS=4 python streaming/event_producer.py          # 4-day stream (CI size)
STREAM_DIR=/tmp/demo python streaming/event_producer.py   # custom output dir
```

Kafka mode (publishes every event to the `retail.events` topic):

```bash
pip install kafka-python
STREAM_BACKEND=kafka KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=retail.events \
  python streaming/event_producer.py
```

Watch events arrive:

```bash
docker exec retail-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic retail.events --max-messages 5
```

## 3. Run Spark Structured Streaming

File replay (no Kafka needed; needs `pip install pyspark`):

```bash
python spark/streaming_job.py --source file --sink-format parquet \
  --trigger-files 48 --timeout 240 --checkpoint spark/checkpoints/
```

- `--sink-format delta` is the production sink (needs the Delta Lake package);
  `parquet` is used for local smoke runs.
- `--trigger-files 48` replays one day of stream per micro-batch.
- `--timeout 240` stops the queries after 4 minutes (omit to run forever).

Smoke test without data:

```bash
python spark/streaming_job.py --source rate
```

Full Kafka source + Kafka alert sink (production topology; needs the Kafka
package for Spark — bump the Scala/version to match your pyspark):

```bash
export KAFKA_BROKERS=localhost:9092
export ALERTS_SINK=kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  spark/streaming_job.py --source kafka --timeout 600
```
(match the package's Spark/Scala version to your installed pyspark)

What the job does: bronze (raw + ingest timestamp) → silver (watermarked
10 min, deduped on `event_id`, quarantined bad rows) → gold hourly revenue
(complete mode) + velocity alerts (30-min customer windows with ≥10 orders).
Checkpoints under `spark/checkpoints/` give exactly-once across restarts —
see `docs/exactly_once.md`.

## 4. Train the ML models

```bash
python ml/train_anomaly.py    # 3-detector IsolationForest system -> ml/artifacts/
python ml/train_forecast.py   # GradientBoosting hourly forecaster -> ml/artifacts/
cat ml/artifacts/anomaly_metrics.json ml/artifacts/forecast_metrics.json
```

Expected on the 7-day stream: fraud burst 100% recall on the injected fraud
scenario, price glitch 100% recall on the injected fraud scenario, ~3% overall
alert rate, forecaster beating the naive baseline (see
`docs/forecast_method.md`).

## 5. Run dbt models and tests

```bash
pip install dbt-duckdb
python scripts/build_warehouse.py     # loads stream + anomaly scores into DuckDB
cd dbt && dbt deps && dbt build --profiles-dir .
```

`dbt build` runs the models (`stg_events` → `fct_orders` → `agg_hourly_revenue`)
**and** all schema tests (uniqueness, not-null, accepted categories, positive
quantities/prices). `dbt test --profiles-dir .` re-runs just the tests.

## 6. View alerts

| Source | Command / location |
|---|---|
| Spark velocity alerts (local run) | `lake/alerts_last_run.txt` (extracted by `./run.sh` from the Spark log) |
| Spark velocity alerts (live) | console output of `spark/streaming_job.py` — rows with `FRAUD_VELOCITY` |
| Kafka alert topic | `docker exec retail-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic retail.alerts --from-beginning` |
| Per-event ML flags | `ml/artifacts/anomaly_scores.csv` (`event_id,is_anomaly,anomaly_detectors`) |
| Dashboard | build the `.pbix` with `powerbi/BUILD_GUIDE.md` |
