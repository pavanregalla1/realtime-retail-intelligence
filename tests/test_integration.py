"""Integration tests — every test runs the REAL code on REAL (small) data.

The module-scoped fixture generates an 8-day stream (~90k events, all three
incidents injected) into a temp dir, then trains both ML models on it.
Spark/dbt tests shell out to the real CLIs and skip if the tool is missing.
"""
import csv
import glob
import json
import os
import shutil
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from streaming.event_producer import main as produce
from ml.train_anomaly import main as train_anomaly
from ml.train_forecast import main as train_forecast

DAYS = 8  # enough history for the forecaster (lags up to 48h) to beat naive


def has_pyspark():
    try:
        __import__("pyspark")
        return True
    except ImportError:
        return False


def has_dbt():
    return shutil.which("dbt") is not None


@pytest.fixture(scope="module")
def mini(tmp_path_factory):
    """Generate an 8-day stream + train both models, all in temp dirs."""
    stream_dir = str(tmp_path_factory.mktemp("stream"))
    art_dir = str(tmp_path_factory.mktemp("artifacts"))
    prod = produce(days=DAYS, out_dir=stream_dir)
    a_metrics = train_anomaly(stream_dir=stream_dir, artifacts_dir=art_dir)
    f_metrics = train_forecast(stream_dir=stream_dir, artifacts_dir=art_dir)
    return {"stream": stream_dir, "art": art_dir,
            "prod": prod, "anomaly": a_metrics, "forecast": f_metrics}


def read_events(stream_dir):
    rows = []
    for f in sorted(glob.glob(os.path.join(stream_dir, "batch_*.jsonl"))):
        with open(f) as fh:
            for line in fh:
                rows.append(json.loads(line))
    return rows


# ---------------------------------------------------------------- producer
def test_producer_writes_expected_batches(mini):
    files = sorted(glob.glob(os.path.join(mini["stream"], "batch_*.jsonl")))
    assert len(files) == DAYS * 48, f"expected {DAYS*48} micro-batch files"
    assert mini["prod"]["events"] > 10_000


def test_producer_injects_fraud_burst(mini):
    rows = read_events(mini["stream"])
    burst = [r for r in rows if r["customer_id"] == "C00999"]
    assert len(burst) >= 40, "fraud burst customer missing from stream"


def test_producer_injects_price_glitch(mini):
    rows = read_events(mini["stream"])
    glitch = [r for r in rows if r["product_id"] == "P0001" and r["unit_price"] < 1]
    assert len(glitch) == 120, f"expected 120 glitch events, got {len(glitch)}"


# ---------------------------------------------------------------- anomaly ML
def test_anomaly_catches_fraud_burst(mini):
    assert mini["anomaly"]["fraud_recall"] >= 0.9


def test_anomaly_catches_price_glitch(mini):
    assert mini["anomaly"]["glitch_recall"] >= 0.9


def test_anomaly_alert_rate_is_small(mini):
    m = mini["anomaly"]
    assert m["anomalies_flagged"] / m["events"] < 0.10


def test_anomaly_scores_csv_written(mini):
    path = os.path.join(mini["art"], "anomaly_scores.csv")
    assert os.path.exists(path)
    with open(path) as fh:
        n = sum(1 for _ in csv.DictReader(fh))
    assert n == mini["anomaly"]["events"]


# ---------------------------------------------------------------- forecast ML
def test_forecast_beats_naive_baseline(mini):
    f = mini["forecast"]
    assert f["backtest_mae"] < f["naive_mae"]
    assert f["improvement_pct"] > 0


def test_forecast_reports_all_metrics(mini):
    for k in ("backtest_mae", "backtest_rmse", "backtest_mape",
              "naive_mae", "naive_rmse", "naive_mape"):
        assert k in mini["forecast"], f"missing metric {k}"


# ---------------------------------------------------------------- Spark
@pytest.mark.skipif(not has_pyspark(), reason="pyspark not installed")
def test_spark_rate_smoke(tmp_path):
    r = subprocess.run(
        [sys.executable, "spark/streaming_job.py", "--source", "rate"],
        cwd=REPO, capture_output=True, text=True, timeout=180)
    assert r.returncode == 0, r.stderr[-2000:]


@pytest.mark.skipif(not has_pyspark(), reason="pyspark not installed")
def test_spark_file_replay_writes_gold(mini, tmp_path):
    chk = str(tmp_path / "chk")
    r = subprocess.run(
        [sys.executable, "spark/streaming_job.py", "--source", "file",
         "--path", mini["stream"] + "/", "--sink-format", "parquet",
         "--trigger-files", "96", "--timeout", "180", "--checkpoint", chk + "/"],
        cwd=REPO, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-2000:]
    silver = os.path.join(REPO, "lake", "silver", "events_clean")
    assert os.path.isdir(silver), "silver lake table was not written"
    assert any(f.endswith(".parquet") for _, _, fs in os.walk(silver) for f in fs)


# ---------------------------------------------------------------- dbt
@pytest.mark.skipif(not has_dbt(), reason="dbt not installed")
def test_dbt_build_and_test(mini, tmp_path):
    warehouse = str(tmp_path / "retail.duckdb")
    env = dict(os.environ, STREAM_DIR=mini["stream"],
               SCORES_CSV=os.path.join(mini["art"], "anomaly_scores.csv"),
               WAREHOUSE=warehouse)
    r = subprocess.run([sys.executable, "scripts/build_warehouse.py"],
                       cwd=REPO, capture_output=True, text=True, env=env, timeout=300)
    assert r.returncode == 0, r.stderr[-2000:]

    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "retail_intelligence:\n  target: dev\n  outputs:\n    dev:\n"
        f"      type: duckdb\n      path: \"{warehouse}\"\n      threads: 2\n")
    dbt_dir = os.path.join(REPO, "dbt")
    if not os.path.isdir(os.path.join(dbt_dir, "dbt_packages")):
        r = subprocess.run(["dbt", "deps", "--project-dir", dbt_dir],
                           capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-2000:]
    r = subprocess.run(["dbt", "build", "--project-dir", dbt_dir,
                        "--profiles-dir", str(profiles)],
                       capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    assert "PASS" in r.stdout or "pass" in r.stdout.lower()
