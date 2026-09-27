#!/usr/bin/env bash
# Real-Time Retail Intelligence — end-to-end pipeline runner.
#
#   ./run.sh [--no-docker] [--days N] [--events N] [--skip-spark] [--skip-dbt]
#
# TWO MODES (see README "Run modes"):
#
#   1. Genuine Kafka path (default when Docker is available):
#      docker compose up -d -> wait for Kafka -> create retail.events /
#      retail.alerts -> producer publishes every event to the Kafka topic
#      (STREAM_BACKEND=both: files are ALSO written for ML/dbt) ->
#      Spark Structured Streaming with --source kafka (startingOffsets=earliest)
#      writing silver/gold to Delta Lake, velocity alerts to the retail.alerts
#      Kafka topic -> ML training -> dbt build+test.
#      NOTE: this path is fully automated here but was NOT executed in this
#      environment (no Docker daemon) — it is code-reviewed, not run-verified.
#
#   2. File-replay fallback (--no-docker): the producer writes micro-batch
#      files, Spark replays them as the stream source (-> Parquet), alerts go
#      to the console. This is the path CI runs on every push (green).
#
# Flags:
#   --no-docker   use the file-replay fallback even if Docker is present
#   --days N      generate N days of stream (default 7)
#   --events N    approximate: generate ~N events (converted to whole days)
#   --skip-spark  skip the Spark streaming step (e.g. pyspark not installed)
#   --skip-dbt    skip the dbt step
#
# Env overrides (Kafka mode):
#   SPARK_PACKAGES  comma-separated --packages for spark-submit
#                   (default: auto-detected from your pyspark version)
#   SPARK_SUBMIT    spark-submit binary (default: spark-submit)
set -euo pipefail
cd "$(dirname "$0")"

DAYS=7
USE_DOCKER=1
SKIP_SPARK=0
SKIP_DBT=0

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-docker) USE_DOCKER=0 ;;
    --days) DAYS="$2"; shift ;;
    --events)
      DAYS=$(( ($2 + 11499) / 11500 )); [[ "$DAYS" -lt 1 ]] && DAYS=1
      echo "note: --events $2 is approximate -> generating $DAYS day(s) of stream"
      shift ;;
    --skip-spark) SKIP_SPARK=1 ;;
    --skip-dbt) SKIP_DBT=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown flag: $1 (try --help)"; exit 1 ;;
  esac
  shift
done

export DAYS
log() { echo -e "\n\033[1m==>\033[0m $*"; }

# ---------------------------------------------------------------- 1. deps
log "Installing Python dependencies"
if ! pip install -q -r requirements.txt 2>/dev/null; then
  echo "WARNING: 'pip install -r requirements.txt' failed (common on"
  echo "Debian/Ubuntu with PEP 668 'externally-managed-environment')."
  echo "Continuing with already-installed packages; if a later step fails,"
  echo "install them manually, e.g.:"
  echo "  pip install --break-system-packages -r requirements.txt"
fi

# ---------------------------------------------------------------- 2. Kafka
KAFKA_MODE=0
if [[ "$USE_DOCKER" -eq 1 ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found -> falling back to --no-docker file replay"; USE_DOCKER=0
  else
    log "Starting Kafka (docker compose up -d) — genuine pipeline mode"
    if ! docker compose up -d; then
      echo "ERROR: 'docker compose up -d' failed."
      echo "Falling back to --no-docker file replay."; USE_DOCKER=0
    else
    log "Waiting for Kafka to be ready"
    READY=0
    for _ in $(seq 1 30); do
      if docker exec retail-kafka cub kafka-ready -b localhost:9092 1 3 >/dev/null 2>&1; then
        READY=1; break
      fi
      sleep 3
    done
    if [[ "$READY" -eq 0 ]]; then
      echo "ERROR: Kafka did not become ready after 90s."
      echo "Falling back to --no-docker file replay."; USE_DOCKER=0
    else
      log "Creating topics retail.events / retail.alerts"
      for T in retail.events retail.alerts; do
        docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
          --create --if-not-exists --topic "$T" --partitions 3 --replication-factor 1 \
          >/dev/null 2>&1 || true
      done
      docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || true
      # kafka-python is required for STREAM_BACKEND=both
      if ! python3 -c "import kafka" 2>/dev/null; then
        log "Installing kafka-python (needed to publish to Kafka)"
        if ! pip install -q kafka-python 2>/dev/null; then
          echo "ERROR: could not install kafka-python — cannot publish to Kafka."
          echo "Falling back to --no-docker file replay."; USE_DOCKER=0
        fi
      fi
    fi
    fi
  fi
fi
if [[ "$USE_DOCKER" -eq 1 ]]; then KAFKA_MODE=1; fi

if [[ "$KAFKA_MODE" -eq 1 ]]; then
  export KAFKA_BROKERS=localhost:9092
  export KAFKA_TOPIC=retail.events
  export ALERTS_SINK=kafka
  export STREAM_BACKEND=both   # files for ML/dbt + Kafka topic for Spark
  SPARK_SOURCE_ARGS="--source kafka --starting-offsets earliest"
  SPARK_SINK="delta"
  SPARK_CHECKPOINT="spark/checkpoints-kafka/"
  echo "MODE: genuine Kafka pipeline (producer -> Kafka -> Spark kafka source -> Delta Lake)"
else
  export STREAM_BACKEND=local
  unset ALERTS_SINK || true
  SPARK_SOURCE_ARGS="--source file"
  SPARK_SINK="parquet"
  SPARK_CHECKPOINT="spark/checkpoints/"
  echo "MODE: file replay (verified path — same transforms, no Kafka)"
fi

# ---------------------------------------------------------------- 3. producer
if [[ "$KAFKA_MODE" -eq 1 ]]; then
  log "Publishing the event stream ($DAYS day(s)) to Kafka topic retail.events (+ files for ML/dbt)"
else
  log "Generating the event stream ($DAYS day(s)) -> data/stream/"
fi
python3 streaming/event_producer.py

# ---------------------------------------------------------------- 4. Spark streaming
# Non-fatal: ML and dbt read data/stream/ directly, so a Spark failure
# (e.g. no Java) must not abort the rest of the pipeline.
if [[ "$SKIP_SPARK" -eq 0 ]] && python3 -c "import pyspark" 2>/dev/null; then
  mkdir -p lake spark
  if [[ "$KAFKA_MODE" -eq 1 ]]; then
    log "Running Spark Structured Streaming (Kafka source -> Delta Lake)"
    # --- package coordinates: auto-detect from the installed pyspark ---
    # The Kafka + Delta jars must match the Spark/Scala version. Override with:
    #   SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,..." ./run.sh
    if [[ -z "${SPARK_PACKAGES:-}" ]]; then
      PYSPARK_VER=$(python3 -c "import pyspark; print(pyspark.__version__)")
      SPARK_MAJOR="${PYSPARK_VER%%.*}"
      if [[ "$SPARK_MAJOR" -ge 4 ]]; then
        SCALA=2.13
        DELTA_VER=4.0.0   # Delta Lake 4.x targets Spark 4.x
      else
        SCALA=2.12
        DELTA_VER=3.2.1   # Delta Lake 3.2.x targets Spark 3.5
      fi
      SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_${SCALA}:${PYSPARK_VER},io.delta:delta-spark_${SCALA}:${DELTA_VER}"
      echo "detected pyspark $PYSPARK_VER -> packages: $SPARK_PACKAGES"
      echo "(override with SPARK_PACKAGES=... if your Spark build needs different coordinates;"
      echo " see docs/runbook.md 'matching package versions')"
    fi
    SUBMIT="${SPARK_SUBMIT:-spark-submit}"
    if ! command -v "$SUBMIT" >/dev/null 2>&1; then
      echo "WARNING: $SUBMIT not found (pip-installed pyspark provides it); skipping Spark."
    elif "$SUBMIT" --packages "$SPARK_PACKAGES" \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        spark/streaming_job.py $SPARK_SOURCE_ARGS --sink-format "$SPARK_SINK" \
        --timeout 600 --checkpoint "$SPARK_CHECKPOINT" \
        2>&1 | tee spark/last_run.log | tail -20; then
      log "Reading velocity alerts back from the retail.alerts Kafka topic"
      docker exec retail-kafka kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic retail.alerts \
        --from-beginning --max-messages 200 --timeout-ms 20000 \
        > lake/alerts_last_run.txt 2>/dev/null || true
      echo "alerts saved to lake/alerts_last_run.txt"
    else
      echo "WARNING: Spark Kafka streaming step failed; continuing without it."
    fi
  else
    log "Running Spark Structured Streaming (file replay -> lake/)"
    if python3 spark/streaming_job.py $SPARK_SOURCE_ARGS --sink-format "$SPARK_SINK" \
        --trigger-files 48 --timeout 240 --checkpoint "$SPARK_CHECKPOINT" \
        2>&1 | tee spark/last_run.log | tail -20; then
      log "Extracting velocity alerts from the Spark log"
      grep -E "FRAUD_VELOCITY|window_start" spark/last_run.log | tail -20 \
        > lake/alerts_last_run.txt || true
      echo "alerts saved to lake/alerts_last_run.txt"
    else
      echo "WARNING: Spark streaming step failed (is Java 11+ installed?); continuing without it."
    fi
  fi
else
  echo "Skipping Spark streaming (pyspark not installed or --skip-spark). Install with: pip install pyspark"
fi

# ---------------------------------------------------------------- 5. ML
log "Training anomaly detectors"
python3 ml/train_anomaly.py
log "Training revenue forecaster"
python3 ml/train_forecast.py

# ---------------------------------------------------------------- 6. dbt (optional)
if [[ "$SKIP_DBT" -eq 0 ]] && command -v dbt >/dev/null 2>&1; then
  log "dbt build + test on DuckDB"
  python3 scripts/build_warehouse.py
  (cd dbt && dbt deps --profiles-dir . >/dev/null && dbt build --profiles-dir .) 2>&1 | tail -25
else
  echo "Skipping dbt (not installed or --skip-dbt). See docs/runbook.md for the manual commands."
fi

# ---------------------------------------------------------------- 7. summary
log "Pipeline complete. Results:"
python3 -c "
import json
a = json.load(open('ml/artifacts/anomaly_metrics.json'))
f = json.load(open('ml/artifacts/forecast_metrics.json'))
print(f\"  events processed : {a['events']:,}\")
print(f\"  fraud burst      : {a['fraud_recall']*100:.0f}% recall on injected fraud scenario\")
print(f\"  price glitch     : {a['glitch_recall']*100:.0f}% recall on injected fraud scenario\")
print(f\"  overall alert rate: {100*a['anomalies_flagged']/a['events']:.2f}% of stream flagged\")
print(f\"  forecast backtest: {f['improvement_pct']}% lower MAE than naive baseline (MAE \${f['backtest_mae']:,.0f} vs \${f['naive_mae']:,.0f})\")
"
echo ""
echo "Where to view alerts:"
if [[ "$KAFKA_MODE" -eq 1 ]]; then
echo "  - Kafka retail.alerts   : lake/alerts_last_run.txt (consumed from the topic above), or live:"
echo "      docker exec retail-kafka kafka-console-consumer.sh \\"
echo "        --bootstrap-server localhost:9092 --topic retail.alerts --from-beginning"
echo "  - Delta tables          : lake/silver/events_clean, lake/gold/hourly_revenue"
else
echo "  - Spark velocity alerts : lake/alerts_last_run.txt (from the streaming run above)"
fi
echo "  - Per-event ML flags    : ml/artifacts/anomaly_scores.csv (event_id, is_anomaly, detectors)"
echo "  - Dashboard             : build the .pbix via powerbi/BUILD_GUIDE.md"
