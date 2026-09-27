#!/usr/bin/env bash
# Real-Time Retail Intelligence — end-to-end pipeline runner.
#
#   ./run.sh [--no-docker] [--days N] [--events N] [--skip-spark] [--skip-dbt]
#
# Steps:
#   1. docker compose up -d, wait for Kafka, create retail.events / retail.alerts
#   2. event producer -> data/stream/ (micro-batch JSON files = the stream)
#   3. Spark Structured Streaming job (file replay) -> lake/silver, lake/gold
#   4. ML: anomaly detectors + revenue forecaster -> ml/artifacts/
#   5. (optional) dbt build + test on DuckDB
#   6. print metrics + where to view alerts
#
# Flags:
#   --no-docker   skip Kafka (pure local run: file replay, console alerts)
#   --days N      generate N days of stream (default 7)
#   --events N    approximate: generate ~N events (converted to whole days)
#   --skip-spark  skip the Spark streaming step (e.g. pyspark not installed)
#   --skip-dbt    skip the dbt step
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
if [[ "$USE_DOCKER" -eq 1 ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found -> continuing without Kafka (as --no-docker)"; USE_DOCKER=0
  else
    log "Starting Kafka (docker compose up -d)"
    docker compose up -d
    log "Waiting for Kafka to be ready"
    READY=0
    for _ in $(seq 1 30); do
      if docker exec retail-kafka cub kafka-ready -b localhost:9092 1 3 >/dev/null 2>&1; then
        READY=1; break
      fi
      sleep 3
    done
    if [[ "$READY" -eq 0 ]]; then echo "Kafka did not become ready; continuing anyway"; fi
    log "Creating topics retail.events / retail.alerts"
    for T in retail.events retail.alerts; do
      docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$T" --partitions 3 --replication-factor 1 \
        >/dev/null 2>&1 || true
    done
    docker exec retail-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || true
  fi
fi

# ---------------------------------------------------------------- 3. producer
log "Generating the event stream ($DAYS day(s)) -> data/stream/"
python3 streaming/event_producer.py

# ---------------------------------------------------------------- 4. Spark streaming
# Non-fatal: ML and dbt read data/stream/ directly, so a Spark failure
# (e.g. no Java) must not abort the rest of the pipeline.
if [[ "$SKIP_SPARK" -eq 0 ]] && python3 -c "import pyspark" 2>/dev/null; then
  log "Running Spark Structured Streaming (file replay -> lake/)"
  mkdir -p spark
  if python3 spark/streaming_job.py --source file --sink-format parquet \
      --trigger-files 48 --timeout 240 --checkpoint spark/checkpoints/ \
      2>&1 | tee spark/last_run.log | tail -20; then
    log "Extracting velocity alerts from the Spark log"
    grep -E "FRAUD_VELOCITY|window_start" spark/last_run.log | tail -20 \
      > lake/alerts_last_run.txt || true
    echo "alerts saved to lake/alerts_last_run.txt"
  else
    echo "WARNING: Spark streaming step failed (is Java 11+ installed?); continuing without it."
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
print(f\"  forecast backtest: {f['improvement_pct']}% better than naive baseline (MAE \${f['backtest_mae']:,.0f} vs \${f['naive_mae']:,.0f})\")
"
echo ""
echo "Where to view alerts:"
echo "  - Spark velocity alerts : lake/alerts_last_run.txt (from the streaming run above)"
echo "  - Per-event ML flags    : ml/artifacts/anomaly_scores.csv (event_id, is_anomaly, detectors)"
if [[ "$USE_DOCKER" -eq 1 ]]; then
echo "  - Kafka retail.alerts   : docker exec retail-kafka kafka-console-consumer.sh \\"
echo "                              --bootstrap-server localhost:9092 --topic retail.alerts --from-beginning"
echo "                            (populated when ALERTS_SINK=kafka, see docs/runbook.md)"
fi
echo "  - Dashboard             : build the .pbix via powerbi/BUILD_GUIDE.md"
