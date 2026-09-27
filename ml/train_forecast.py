"""Hourly revenue forecaster (genuinely runs here).

Aggregates the event stream to hourly revenue, engineers lag/rolling/calendar
features, trains a GradientBoostingRegressor, and backtests on the last 24h.
Artifacts + metrics -> ml/artifacts/.
"""
import json, glob, os
import pandas as pd, numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
import joblib

rows = []
for f in sorted(glob.glob("data/stream/batch_*.jsonl")):
    for line in open(f):
        rows.append(json.loads(line))
df = pd.DataFrame(rows)
df["event_time"] = pd.to_datetime(df["event_time"])

hourly = (df.set_index("event_time")["line_total"]
            .resample("h").sum().asfreq("h", fill_value=0).to_frame("revenue"))
hourly["hour"] = hourly.index.hour
hourly["dow"] = hourly.index.dayofweek
hourly["is_weekend"] = (hourly["dow"] >= 5).astype(int)
for lag in (1, 2, 3, 24, 48):
    hourly[f"lag_{lag}"] = hourly["revenue"].shift(lag)
hourly["roll24_mean"] = hourly["revenue"].shift(1).rolling(24).mean()
hourly["roll24_max"] = hourly["revenue"].shift(1).rolling(24).max()
hourly = hourly.dropna()
print(f"hourly rows={len(hourly)}, total revenue=${hourly['revenue'].sum():,.0f}")

FEATS = [c for c in hourly.columns if c != "revenue"]
train, test = hourly.iloc[:-24], hourly.iloc[-24:]   # backtest: last 24h
model = GradientBoostingRegressor(n_estimators=300, max_depth=4,
                                  learning_rate=0.05, random_state=42)
model.fit(train[FEATS], train["revenue"])
pred = model.predict(test[FEATS])
mae = mean_absolute_error(test["revenue"], test["revenue"] * 0 + test["revenue"].mean())
model_mae = mean_absolute_error(test["revenue"], pred)
base_mae = mean_absolute_error(test["revenue"],
                               np.full(len(test), train["revenue"].mean()))
print(f"backtest MAE: model=${model_mae:,.0f} vs naive-mean=${base_mae:,.0f} "
      f"({100*(1-model_mae/base_mae):.0f}% better)")

os.makedirs("ml/artifacts", exist_ok=True)
joblib.dump({"model": model, "features": FEATS}, "ml/artifacts/forecast_model.pkl")
json.dump({"backtest_mae": round(model_mae, 2),
           "naive_mae": round(base_mae, 2),
           "improvement_pct": round(100*(1-model_mae/base_mae), 1),
           "last_6h_actual": [round(float(x), 2) for x in test["revenue"].iloc[-6:]],
           "last_6h_predicted": [round(float(x), 2) for x in pred[-6:]]},
          open("ml/artifacts/forecast_metrics.json", "w"), indent=2)
assert model_mae < base_mae, "model must beat naive baseline"
print("OK: forecaster beats naive baseline")
