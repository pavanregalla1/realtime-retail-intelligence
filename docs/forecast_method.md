# Forecast method — how the "83% better" number is produced

Script: `ml/train_forecast.py`. All numbers below are from a real run on the
7-day stream (2026-09-26); the script prints them and stores them in
`ml/artifacts/forecast_metrics.json`.

## Data

Events are aggregated to **hourly revenue** (168 hourly buckets over 7 days).
After feature engineering (lags up to 48h + 24h rolling stats), `dropna()`
leaves 120 usable hourly rows totaling **$38,183,945**.

## Features

`hour`, `dayofweek`, `is_weekend`, `lag_1/2/3/24/48`, `roll24_mean`, `roll24_max`.

## Model

`GradientBoostingRegressor(n_estimators=300, max_depth=4, learning_rate=0.05,
random_state=42)` — deterministic (`random_state=42`), so re-runs reproduce
these numbers.

## Backtest protocol

- **Train:** first 96 usable hours. **Test:** last 24 hours (the final day).
- **Naive baseline:** predict the *training-set mean* for every test hour —
  the standard "no-skill" reference for a seasonal series.

## Results (last-24h backtest)

| Metric | Model | Naive baseline |
|---|---|---|
| MAE  | **$20,604** | $123,755 |
| RMSE | **$27,844** | $157,338 |
| MAPE | **9.2%** | 117.0% |

## Improvement formula

```
improvement = 1 − (model_MAE / naive_MAE)
            = 1 − (20,604 / 123,755)
            = 0.834  →  83.4% better than the naive baseline
```

The naive baseline is deliberately weak (a flat mean cannot follow the
evening demand peak or the day-3 flash sale), so "83% better" means the model
captures the real daily seasonality — it is a sanity signal, not a claim of
production accuracy. See the README's *Results and Limitations* section.
