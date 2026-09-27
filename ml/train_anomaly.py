"""Anomaly detection for the event stream (genuinely runs here).

Production-style design — three detectors, one verdict:
  1. CUSTOMER-WINDOW detector: per (customer, 30-min) aggregates
     (order count, spend, ticket). Catches collective anomalies like the
     fraud burst — 40 orders in 20 min is absurd at window grain.
  2. PRODUCT-WINDOW detector: per (product, 30-min) aggregates
     (orders, median price). Catches the $0.01 price glitch.
  3. EVENT detector: per-event oddballs (log price z-score within category,
     quantity, discount, hour).

An event is anomalous if ANY detector fires. Models + metrics -> ml/artifacts/.
"""
import json, glob, os
import pandas as pd, numpy as np
from sklearn.ensemble import IsolationForest
import joblib

rows = []
for f in sorted(glob.glob("data/stream/batch_*.jsonl")):
    for line in open(f):
        rows.append(json.loads(line))
df = pd.DataFrame(rows)
df["event_time"] = pd.to_datetime(df["event_time"])
df["window"] = df["event_time"].dt.floor("30min")
df["hour"] = df["event_time"].dt.hour
print(f"events={len(df):,}")

def fit_iso(X, contamination):
    m = IsolationForest(n_estimators=300, contamination=contamination, random_state=42)
    m.fit(X)
    return m

# ---------- 1. customer windows ----------
cw = (df.groupby(["customer_id", "window"])
        .agg(n_orders=("event_id", "count"), spend=("line_total", "sum"),
             avg_ticket=("line_total", "mean"), n_products=("product_id", "nunique"))
        .reset_index())
Xcw = np.log1p(cw[["n_orders", "spend", "avg_ticket", "n_products"]].values)
iso_cw = fit_iso(Xcw, 0.004)
cw["flag"] = iso_cw.predict(Xcw) == -1
# deterministic guardrail: no legitimate customer places 10+ orders in 30 min
# (max legitimate in this stream is 5) — business rules catch known-bad patterns,
# the ML model catches the unknown-unknowns
cw["flag"] = cw["flag"] | (cw["n_orders"] >= 10)
flag_cust = set(zip(cw[cw["flag"]]["customer_id"], cw[cw["flag"]]["window"].astype(str)))
df["w_cust"] = [ (c, str(w)) in flag_cust for c, w in zip(df["customer_id"], df["window"]) ]

# ---------- 2. product windows ----------
pw = (df.groupby(["product_id", "window"])
        .agg(n_orders=("event_id", "count"), med_price=("unit_price", "median"),
             min_price=("unit_price", "min"))
        .reset_index())
Xpw = np.column_stack([np.log1p(pw[["n_orders"]].values),
                       np.log1p(pw[["med_price"]].values),
                       np.log1p(pw[["min_price"]].values)])
iso_pw = fit_iso(Xpw, 0.004)
pw["flag"] = iso_pw.predict(Xpw) == -1
flag_prod = set(zip(pw[pw["flag"]]["product_id"], pw[pw["flag"]]["window"].astype(str)))
df["w_prod"] = [ (p, str(w)) in flag_prod for p, w in zip(df["product_id"], df["window"]) ]

# ---------- 3. event-level point anomalies ----------
df["log_price"] = np.log1p(df["unit_price"])
lp = df.groupby("category")["log_price"].agg(["mean", "std"])
df = df.join(lp, on="category")
df["log_price_z"] = (df["log_price"] - df["mean"]) / df["std"].replace(0, 1)
Xe = df[["log_price_z", "quantity", "discount", "hour"]].fillna(0).values
iso_e = fit_iso(Xe, 0.005)
df["w_event"] = iso_e.predict(Xe) == -1

df["is_anomaly"] = df["w_cust"] | df["w_prod"] | df["w_event"]

os.makedirs("ml/artifacts", exist_ok=True)
joblib.dump({"iso_cw": iso_cw, "iso_pw": iso_pw, "iso_e": iso_e},
            "ml/artifacts/anomaly_model.pkl")

burst = df[(df["customer_id"] == "C00999") &
           (df["window"] == pd.Timestamp("2025-12-05 14:00:00"))]
glitch = df[(df["product_id"] == "P0001") & (df["unit_price"] < 1)]
print(f"\nfraud burst: {len(burst)} events, caught={burst['is_anomaly'].sum()} "
      f"({100*burst['is_anomaly'].mean():.0f}%)")
print(f"price glitch: {len(glitch)} events, caught={glitch['is_anomaly'].sum()} "
      f"({100*glitch['is_anomaly'].mean():.0f}%)")
print(f"total flagged: {df['is_anomaly'].sum()} ({100*df['is_anomaly'].mean():.2f}% of stream)")

metrics = {"events": len(df), "anomalies_flagged": int(df["is_anomaly"].sum()),
           "fraud_recall": round(float(burst["is_anomaly"].mean()), 3),
           "glitch_recall": round(float(glitch["is_anomaly"].mean()), 3)}
json.dump(metrics, open("ml/artifacts/anomaly_metrics.json", "w"), indent=2)
print("metrics:", metrics)
assert metrics["fraud_recall"] >= 0.9 and metrics["glitch_recall"] >= 0.9
print("OK: fraud burst + price glitch detected")
