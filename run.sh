#!/usr/bin/env bash
# End-to-end demo: generate stream -> train anomaly detectors -> train forecaster
set -e
cd "$(dirname "$0")"
pip install -r requirements.txt -q
python3 streaming/event_producer.py
python3 ml/train_anomaly.py
python3 ml/train_forecast.py
echo ""
echo "Done. Metrics:"
cat ml/artifacts/anomaly_metrics.json ml/artifacts/forecast_metrics.json
