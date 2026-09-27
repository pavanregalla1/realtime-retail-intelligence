"""Render evidence screenshots from REAL pipeline outputs (no invented numbers).

Inputs (all produced by real runs):
  data/stream/batch_*.jsonl        the event stream
  ml/artifacts/anomaly_metrics.json, anomaly_scores.csv, forecast_metrics.json
  spark/batch_stats.jsonl          per-micro-batch stats from a real Spark run
                                   (streaming_job.py --progress-log)
  dbt/build_output.log             captured `dbt build` output (see docs/runbook.md)

Outputs: docs/images/*.png — embedded in the README.

Usage: python docs/render_evidence.py [--dbt-log dbt/build_output.log]
"""
import argparse
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMG = os.path.join(REPO, "docs", "images")
os.makedirs(IMG, exist_ok=True)
plt.rcParams.update({"figure.dpi": 130, "font.size": 9})


def need(path):
    if not os.path.exists(path):
        sys.exit(f"missing required input: {path} — run the pipeline first (see docs/runbook.md)")
    return path


def load_stream():
    rows = []
    for f in sorted(glob.glob(os.path.join(REPO, "data/stream/batch_*.jsonl"))):
        with open(f) as fh:
            for line in fh:
                rows.append(json.loads(line))
    if not rows:
        sys.exit("no stream files found in data/stream/")
    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df


# ------------------------------------------------ 1. kafka events sample
def kafka_events(df):
    sample = df.head(12)[["event_time", "event_id", "customer_id",
                          "product_id", "quantity", "unit_price", "line_total"]]
    sample["event_id"] = sample["event_id"].str.slice(0, 8) + "…"
    sample["event_time"] = sample["event_time"].dt.strftime("%H:%M:%S")
    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.axis("off")
    ax.set_title("Sample of real events emitted by streaming/event_producer.py\n"
                 "(identical JSON payload published to Kafka topic retail.events in prod mode)",
                 fontsize=10, pad=12)
    t = ax.table(cellText=sample.values, colLabels=sample.columns,
                 loc="center", colWidths=[0.10, 0.16, 0.14, 0.12, 0.10, 0.12, 0.12])
    t.auto_set_font_size(False); t.set_fontsize(7); t.scale(1, 1.25)
    fig.tight_layout(); fig.savefig(f"{IMG}/kafka_events.png", bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------ 2/6. spark batches + latency
def spark_evidence():
    stats = [json.loads(l) for l in open(need(f"{REPO}/spark/batch_stats.jsonl"))]
    if not stats:
        sys.exit("spark/batch_stats.jsonl is empty — re-run streaming_job.py with --progress-log")
    s = pd.DataFrame(stats)
    s = s[s["query"] == "silver"].reset_index(drop=True)  # main data path
    if s.empty:
        sys.exit("no batches recorded for the silver query")
    # batches table
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.axis("off")
    ax.set_title("Spark Structured Streaming — real micro-batches, silver query (file replay)\n"
                 f"{len(s)} batches, {s['input_rows'].sum():,} rows processed", fontsize=10, pad=12)
    show = s.head(12)[["batch_id", "input_rows", "duration_ms"]]
    t = ax.table(cellText=show.values, colLabels=["batch_id", "input_rows", "duration_ms"],
                 loc="center", colWidths=[0.25, 0.35, 0.35])
    t.auto_set_font_size(False); t.set_fontsize(8); t.scale(1, 1.3)
    fig.tight_layout(); fig.savefig(f"{IMG}/spark_batches.png", bbox_inches="tight")
    plt.close(fig)
    # latency chart
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.bar(s["batch_id"], s["duration_ms"] / 1000.0, color="#1f77b4")
    ax.set_xlabel("micro-batch id"); ax.set_ylabel("trigger execution (s)")
    ax.set_title(f"Micro-batch processing latency — median "
                 f"{s['duration_ms'].median()/1000:.2f}s, p95 {s['duration_ms'].quantile(0.95)/1000:.2f}s")
    fig.tight_layout(); fig.savefig(f"{IMG}/latency.png", bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------ 3. fraud alerts
def fraud_alerts(df):
    m = json.load(open(need(f"{REPO}/ml/artifacts/anomaly_metrics.json")))
    scores = pd.read_csv(need(f"{REPO}/ml/artifacts/anomaly_scores.csv"))
    flagged = scores[scores["is_anomaly"]].merge(
        df[["event_id", "event_time", "customer_id", "product_id",
            "quantity", "unit_price", "line_total"]], on="event_id", how="left")
    top = flagged.nlargest(10, "line_total")[
        ["event_time", "customer_id", "product_id", "quantity", "unit_price",
         "line_total", "anomaly_detectors"]]
    top["event_time"] = pd.to_datetime(top["event_time"]).dt.strftime("%m-%d %H:%M")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 6),
                                   gridspec_kw={"height_ratios": [1, 3]})
    ax1.axis("off")
    ax1.text(0.02, 0.75,
             f"Anomaly detection on the real 7-day stream — {m['events']:,} events",
             fontsize=11, weight="bold", transform=ax1.transAxes)
    ax1.text(0.02, 0.35,
             f"Fraud burst (40 orders/20 min, C00999): {m['fraud_recall']*100:.0f}% recall on injected fraud scenario\n"
             f"Price glitch (P0001 @ $0.01, 120 orders): {m['glitch_recall']*100:.0f}% recall on injected fraud scenario\n"
             f"Overall alert rate: {100*m['anomalies_flagged']/m['events']:.2f}% of stream flagged "
             f"({m['anomalies_flagged']:,} alerts)",
             fontsize=9, transform=ax1.transAxes, va="top")
    ax2.axis("off")
    ax2.set_title("Top flagged events by line total (real detector output)", fontsize=10, pad=8)
    t = ax2.table(cellText=top.values, colLabels=top.columns, loc="center")
    t.auto_set_font_size(False); t.set_fontsize(6.5); t.scale(1, 1.15)
    fig.tight_layout(); fig.savefig(f"{IMG}/fraud_alerts.png", bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------ 4. forecast chart
def forecast_backtest():
    from sklearn.ensemble import GradientBoostingRegressor
    df = load_stream()
    h = (df.set_index("event_time")["line_total"]
           .resample("h").sum().asfreq("h", fill_value=0).to_frame("revenue"))
    h["hour"] = h.index.hour; h["dow"] = h.index.dayofweek
    h["is_weekend"] = (h["dow"] >= 5).astype(int)
    for lag in (1, 2, 3, 24, 48):
        h[f"lag_{lag}"] = h["revenue"].shift(lag)
    h["roll24_mean"] = h["revenue"].shift(1).rolling(24).mean()
    h["roll24_max"] = h["revenue"].shift(1).rolling(24).max()
    h = h.dropna()
    feats = [c for c in h.columns if c != "revenue"]
    train, test = h.iloc[:-24], h.iloc[-24:]
    model = GradientBoostingRegressor(n_estimators=300, max_depth=4,
                                      learning_rate=0.05, random_state=42)
    model.fit(train[feats], train["revenue"])
    pred = model.predict(test[feats])
    m = json.load(open(need(f"{REPO}/ml/artifacts/forecast_metrics.json")))
    fig, ax = plt.subplots(figsize=(10, 3.8))
    x = np.arange(len(test))
    ax.plot(x, test["revenue"].to_numpy(), label="actual", color="#1f77b4", lw=1.5)
    ax.plot(x, pred, label="forecast", color="#ff7f0e", lw=1.5, ls="--")
    ax.set_xlabel("backtest hour (last 24h)"); ax.set_ylabel("revenue ($)")
    ax.set_title(f"Revenue forecast backtest — MAE ${m['backtest_mae']:,.0f} vs "
                 f"naive ${m['naive_mae']:,.0f} ({m['improvement_pct']}% better)")
    ax.legend(); ax.ticklabel_format(style="plain", axis="y")
    fig.tight_layout(); fig.savefig(f"{IMG}/forecast.png", bbox_inches="tight")
    plt.close(fig)
    # dashboard-styled twin
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.fill_between(x, test["revenue"].to_numpy(), alpha=0.25, color="#1f77b4")
    ax.plot(x, test["revenue"].to_numpy(), color="#1f77b4", lw=2)
    ax.plot(x, pred, color="#ff7f0e", lw=2, ls="--", label="model forecast")
    ax.set_title("Dashboard view — forecast vs actual (rendered from the same data)")
    ax.legend(); fig.tight_layout()
    fig.savefig(f"{IMG}/dashboard_forecast.png", bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------ 5. dbt test output
def dbt_tests(log_path):
    lines = open(need(log_path)).read().splitlines()
    tail = lines[-28:]
    fig, ax = plt.subplots(figsize=(10, 6.5))
    ax.axis("off")
    ax.set_title("Real `dbt build` output — models + tests (DuckDB)", fontsize=10, pad=10,
                 family="monospace")
    ax.text(0.02, 0.96, "\n".join(tail), fontsize=6.5, family="monospace",
            va="top", ha="left", transform=ax.transAxes,
            bbox=dict(boxstyle="round", facecolor="#0d1117", edgecolor="#30363d"))
    fig.patch.set_facecolor("white")
    fig.tight_layout(); fig.savefig(f"{IMG}/dbt_tests.png", bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------ 7/8. dashboard-style visuals
def dashboard_visuals(df):
    # throughput pulse
    df["hour"] = df["event_time"].dt.floor("h")
    per_hour = df.groupby("hour").size()
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.fill_between(per_hour.index, per_hour.values, alpha=0.3, color="#1f77b4")
    ax.plot(per_hour.index, per_hour.values, color="#1f77b4", lw=1.2)
    ax.set_title("Dashboard view — event throughput per hour, 7 days (rendered from the same data)")
    ax.set_ylabel("events/hour"); fig.autofmt_xdate()
    fig.tight_layout(); fig.savefig(f"{IMG}/dashboard_throughput.png", bbox_inches="tight")
    plt.close(fig)
    # anomaly center: flagged events over time
    scores = pd.read_csv(need(f"{REPO}/ml/artifacts/anomaly_scores.csv"))
    flagged = scores[scores["is_anomaly"]].merge(df[["event_id", "event_time"]],
                                                 on="event_id", how="left")
    flagged["hour"] = pd.to_datetime(flagged["event_time"]).dt.floor("h")
    per_h = flagged.groupby("hour").size()
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.bar(per_h.index, per_h.values, width=0.04, color="#d62728")
    ax.set_title("Dashboard view — flagged events per hour: fraud burst + price glitch visible "
                 "(rendered from the same data)")
    ax.set_ylabel("alerts/hour"); fig.autofmt_xdate()
    fig.tight_layout(); fig.savefig(f"{IMG}/dashboard_anomalies.png", bbox_inches="tight")
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbt-log", default=f"{REPO}/dbt/build_output.log")
    args = ap.parse_args()
    df = load_stream()
    print("rendering kafka_events.png ..."); kafka_events(df)
    print("rendering spark_batches.png + latency.png ..."); spark_evidence()
    print("rendering fraud_alerts.png ..."); fraud_alerts(df)
    print("rendering forecast.png ..."); forecast_backtest()
    print("rendering dbt_tests.png ..."); dbt_tests(args.dbt_log)
    print("rendering dashboard visuals ..."); dashboard_visuals(df)
    print("done ->", IMG)


if __name__ == "__main__":
    main()
