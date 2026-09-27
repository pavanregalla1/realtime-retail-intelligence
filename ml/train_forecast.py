"""Hourly revenue forecaster (genuinely runs here).

Aggregates the event stream to hourly revenue, engineers lag/rolling/calendar
features, trains a GradientBoostingRegressor, and backtests on the last 24h
against a naive baseline (predict the training-set mean every hour).
Artifacts + metrics -> ml/artifacts/.

See docs/forecast_method.md for the full method, formulas and numbers.
"""
import json, glob, os
import pandas as pd, numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib


def mape(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    mask = y_true != 0
    return float(np.mean(np.abs((y_true[mask] - np.asarray(y_pred)[mask]) / y_true[mask])))


def main(stream_dir=None, artifacts_dir="ml/artifacts"):
    stream_dir = stream_dir or os.environ.get("STREAM_DIR", "data/stream")
    rows = []
    for f in sorted(glob.glob(f"{stream_dir}/batch_*.jsonl")):
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
    naive = np.full(len(test), train["revenue"].mean())

    y = test["revenue"].to_numpy()
    m = {
        "model_mae": float(mean_absolute_error(y, pred)),
        "model_rmse": float(mean_squared_error(y, pred) ** 0.5),
        "model_mape": mape(y, pred),
        "naive_mae": float(mean_absolute_error(y, naive)),
        "naive_rmse": float(mean_squared_error(y, naive) ** 0.5),
        "naive_mape": mape(y, naive),
    }
    m["improvement_pct"] = round(100 * (1 - m["model_mae"] / m["naive_mae"]), 1)
    print(f"backtest MAE : model=${m['model_mae']:,.0f} vs naive-mean=${m['naive_mae']:,.0f} "
          f"({m['improvement_pct']}% better)")
    print(f"backtest RMSE: model=${m['model_rmse']:,.0f} vs naive-mean=${m['naive_rmse']:,.0f}")
    print(f"backtest MAPE: model={100*m['model_mape']:.1f}% vs naive-mean={100*m['naive_mape']:.1f}%")

    os.makedirs(artifacts_dir, exist_ok=True)
    joblib.dump({"model": model, "features": FEATS}, f"{artifacts_dir}/forecast_model.pkl")
    out = {
        "backtest_mae": round(m["model_mae"], 2),
        "backtest_rmse": round(m["model_rmse"], 2),
        "backtest_mape": round(m["model_mape"], 4),
        "naive_mae": round(m["naive_mae"], 2),
        "naive_rmse": round(m["naive_rmse"], 2),
        "naive_mape": round(m["naive_mape"], 4),
        "improvement_pct": m["improvement_pct"],
        "backtest_hours": len(test),
        "last_6h_actual": [round(float(x), 2) for x in test["revenue"].iloc[-6:]],
        "last_6h_predicted": [round(float(x), 2) for x in pred[-6:]],
    }
    json.dump(out, open(f"{artifacts_dir}/forecast_metrics.json", "w"), indent=2)
    assert m["model_mae"] < m["naive_mae"], "model must beat naive baseline"
    print("OK: forecaster beats naive baseline")
    return out


if __name__ == "__main__":
    main()
