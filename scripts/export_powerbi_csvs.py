"""Export real pipeline outputs as CSVs for Power BI Desktop.

Reads the event stream + ML artifacts and writes:
  powerbi/data/hourly_revenue.csv   hour,category,orders,revenue,avg_ticket
  powerbi/data/anomaly_alerts.csv   window_start,customer_id,orders_30min,
                                    spend_30min,alert_type  (velocity guardrail,
                                    same rule as the Spark streaming job)
  powerbi/data/forecast.csv         hour,actual_revenue,predicted_revenue
                                    (24h backtest from ml/train_forecast.py)

Run:  python scripts/export_powerbi_csvs.py   (after the ML training steps)
Then follow powerbi/BUILD_GUIDE.md in Power BI Desktop.
"""
import glob
import json
import os

import pandas as pd

STREAM_DIR = os.environ.get("STREAM_DIR", "data/stream")
OUT = "powerbi/data"
os.makedirs(OUT, exist_ok=True)

rows = []
for f in sorted(glob.glob(f"{STREAM_DIR}/batch_*.jsonl")):
    for line in open(f):
        rows.append(json.loads(line))
df = pd.DataFrame(rows)
df["event_time"] = pd.to_datetime(df["event_time"])
print(f"events={len(df):,}")

# ---- 1. hourly revenue by category (gold.agg_hourly_revenue equivalent) ----
df["hour"] = df["event_time"].dt.floor("h")
hourly = (df.groupby(["hour", "category"])
            .agg(orders=("event_id", "count"),
                 revenue=("line_total", "sum"),
                 avg_ticket=("line_total", "mean"))
            .reset_index())
hourly["revenue"] = hourly["revenue"].round(2)
hourly["avg_ticket"] = hourly["avg_ticket"].round(2)
hourly.to_csv(f"{OUT}/hourly_revenue.csv", index=False)
print(f"hourly_revenue.csv: {len(hourly):,} rows")

# ---- 2. velocity alerts (same >=10-orders-per-30min rule as Spark) ----
df["window"] = df["event_time"].dt.floor("30min")
vel = (df.groupby(["window", "customer_id"])
         .agg(orders_30min=("event_id", "count"),
              spend_30min=("line_total", "sum"))
         .reset_index())
alerts = vel[vel["orders_30min"] >= 10].copy()
alerts["alert_type"] = "FRAUD_VELOCITY"
alerts = alerts.rename(columns={"window": "window_start"})
alerts[["window_start", "customer_id", "orders_30min",
        "spend_30min", "alert_type"]].to_csv(f"{OUT}/anomaly_alerts.csv", index=False)
print(f"anomaly_alerts.csv: {len(alerts):,} alerts")

# ---- 3. forecast backtest: actual vs predicted per hour ----
import numpy as np  # noqa: E402
from sklearn.ensemble import GradientBoostingRegressor  # noqa: E402

h = (df.set_index("event_time")["line_total"]
       .resample("h").sum().asfreq("h", fill_value=0).to_frame("revenue"))
h["hour"] = h.index.hour
h["dow"] = h.index.dayofweek
h["is_weekend"] = (h["dow"] >= 5).astype(int)
for lag in (1, 2, 3, 24, 48):
    h[f"lag_{lag}"] = h["revenue"].shift(lag)
h["roll24_mean"] = h["revenue"].shift(1).rolling(24).mean()
h["roll24_max"] = h["revenue"].shift(1).rolling(24).max()
h = h.dropna()
FEATS = [c for c in h.columns if c != "revenue"]
train, test = h.iloc[:-24], h.iloc[-24:]
model = GradientBoostingRegressor(n_estimators=300, max_depth=4,
                                  learning_rate=0.05, random_state=42)
model.fit(train[FEATS], train["revenue"])
fc = pd.DataFrame({"hour": test.index,
                   "actual_revenue": test["revenue"].round(2),
                   "predicted_revenue": model.predict(test[FEATS]).round(2)})
fc.to_csv(f"{OUT}/forecast.csv", index=False)
print(f"forecast.csv: {len(fc):,} rows (24h backtest)")
print("done ->", OUT)
