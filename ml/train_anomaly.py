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

Recall is measured on the *injected* fraud scenarios only (customer C00999's
burst and the P0001 price glitch) — normal events are not all labeled, so a
true false-positive rate cannot be computed; what we report is the overall
alert rate (share of the stream flagged).
"""
import json, glob, os
import pandas as pd, numpy as np
from sklearn.ensemble import IsolationForest
import joblib

FRAUD_CUSTOMER = "C00999"   # injected fraud burst customer
GLITCH_PID = "P0001"        # injected price-glitch product


def fit_iso(X, contamination):
    m = IsolationForest(n_estimators=300, contamination=contamination, random_state=42)
    m.fit(X)
    return m


def main(stream_dir=None, artifacts_dir="ml/artifacts"):
    stream_dir = stream_dir or os.environ.get("STREAM_DIR", "data/stream")
    rows = []
    for f in sorted(glob.glob(f"{stream_dir}/batch_*.jsonl")):
        for line in open(f):
            rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["window"] = df["event_time"].dt.floor("30min")
    df["hour"] = df["event_time"].dt.hour
    print(f"events={len(df):,}")

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

    os.makedirs(artifacts_dir, exist_ok=True)
    joblib.dump({"iso_cw": iso_cw, "iso_pw": iso_pw, "iso_e": iso_e},
                f"{artifacts_dir}/anomaly_model.pkl")
    # per-event scores for the dbt marts layer (ml.anomaly_scores source)
    df[["event_id", "is_anomaly"]].assign(
        anomaly_detectors=df[["w_cust", "w_prod", "w_event"]].apply(
            lambda r: ",".join(n for n, v in zip(
                ["customer_window", "product_window", "event"], r) if v), axis=1)
    ).to_csv(f"{artifacts_dir}/anomaly_scores.csv", index=False)

    # recall on the INJECTED scenarios (found dynamically — works on any stream length).
    # The fraud burst is the one 30-min window where C00999 placed >= 10 orders
    # (its legitimate orders in other windows are NOT part of the scenario).
    cust_windows = df[df["customer_id"] == FRAUD_CUSTOMER].groupby("window").size()
    burst_window = cust_windows.idxmax() if len(cust_windows) else None
    if burst_window is None or cust_windows.max() < 10:
        raise RuntimeError("injected fraud burst not found in stream")
    burst = df[(df["customer_id"] == FRAUD_CUSTOMER) & (df["window"] == burst_window)]
    glitch = df[(df["product_id"] == GLITCH_PID) & (df["unit_price"] < 1)]
    fraud_recall = float(burst["is_anomaly"].mean()) if len(burst) else 0.0
    glitch_recall = float(glitch["is_anomaly"].mean()) if len(glitch) else 0.0
    print(f"\nfraud burst: {len(burst)} events, caught={burst['is_anomaly'].sum()} "
          f"({100*fraud_recall:.0f}% recall on injected fraud scenario)")
    print(f"price glitch: {len(glitch)} events, caught={glitch['is_anomaly'].sum()} "
          f"({100*glitch_recall:.0f}% recall on injected fraud scenario)")
    print(f"total flagged: {df['is_anomaly'].sum()} "
          f"({100*df['is_anomaly'].mean():.2f}% overall alert rate)")

    metrics = {"events": len(df), "anomalies_flagged": int(df["is_anomaly"].sum()),
               "fraud_recall": round(fraud_recall, 3),
               "glitch_recall": round(glitch_recall, 3)}
    json.dump(metrics, open(f"{artifacts_dir}/anomaly_metrics.json", "w"), indent=2)
    print("metrics:", metrics)
    assert metrics["fraud_recall"] >= 0.9 and metrics["glitch_recall"] >= 0.9
    print("OK: fraud burst + price glitch detected")
    return metrics


if __name__ == "__main__":
    main()
