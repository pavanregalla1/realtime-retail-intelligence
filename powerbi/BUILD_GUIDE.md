# Build the Power BI dashboard (.pbix) — ~10 minutes

Power BI Desktop is Windows-only, so the `.pbix` can't be built on Linux/macOS
CI. Instead: generate real data CSVs from the pipeline, then follow these
click-by-click steps in Power BI Desktop. The result is
`powerbi/realtime_retail_dashboard.pbix` (gitignored — build it locally).

## 0. Generate the data (2 min)

On any machine with Python:

```bash
pip install -r requirements.txt
python streaming/event_producer.py
python ml/train_anomaly.py
python ml/train_forecast.py
python scripts/export_powerbi_csvs.py
```

This creates `powerbi/data/` with three real CSVs:

| File | Contents |
|---|---|
| `hourly_revenue.csv` | hour, category, orders, revenue, avg_ticket (real stream aggregation) |
| `anomaly_alerts.csv` | velocity alerts — the ≥10-orders-per-30-min rule, same as the Spark job |
| `forecast.csv` | 24h backtest: actual vs predicted hourly revenue |

Copy the `powerbi/data/` folder to your Windows machine (or regenerate it there).

## 1. Load the data (2 min)

1. Open Power BI Desktop → **Get Data → Text/CSV**.
2. Load `hourly_revenue.csv` → in the preview, set `hour` to **Date/Time** type → **Load**.
3. Repeat for `anomaly_alerts.csv` (set `window_start` to Date/Time) and `forecast.csv` (set `hour` to Date/Time).
4. **Model view**: no relationships needed — each page uses one table.

## 2. Page 1 — Live Revenue Pulse (3 min)

Rename Page 1 to `Revenue Pulse`. Add:

1. **Card** → `revenue` → rename visual title to `Revenue (all hours)`. Format → Callout value → Display units: Thousands.
2. **Card** → `orders` → title `Orders (all hours)`.
3. **Card** → from `anomaly_alerts`, field `alert_type`, aggregation **Count** → title `Active velocity alerts`.
4. **Line chart** → X-axis: `hour`, Y-axis: `revenue`. Title: `Revenue per hour`.
5. **Bar chart** → Y-axis: `category`, X-axis: `revenue`. Title: `Revenue by category`.
6. Insert → **Text box** → paste: `Source: event stream via scripts/export_powerbi_csvs.py`.

Optional DAX measure (Modeling → New measure):

```dax
Revenue Last Hour =
CALCULATE ( SUM ( hourly_revenue[revenue] ), hourly_revenue[hour] = MAX ( hourly_revenue[hour] ) )
```

## 3. Page 2 — Anomaly Command Center (2 min)

Rename Page 2 to `Anomaly Center`. Add:

1. **Table** → `window_start`, `customer_id`, `orders_30min`, `spend_30min`, `alert_type`. Sort by `window_start` descending. Title: `Velocity alert feed`.
2. **Gauge** → Value: count of `alert_type`; Target value: type `40`. Title: `Alerts vs worst-case burst`.
3. **Card** → `spend_30min`, aggregation **Maximum** → title `Largest 30-min customer spend`.

## 4. Page 3 — Forecast vs Actual (2 min)

Rename Page 3 to `Forecast`. Add:

1. **Line chart** → X-axis: `hour`; Y-axis: `actual_revenue` and `predicted_revenue`. Title: `24h backtest — actual vs forecast`.
2. **Card** → new DAX measure:

```dax
Backtest MAE = AVERAGEX ( forecast, ABS ( forecast[actual_revenue] - forecast[predicted_revenue] ) )
```

3. **Text box** → `Model: GradientBoostingRegressor, 83.4% better than naive-mean baseline (see docs/forecast_method.md).`

## 5. Save

**File → Save As** → `powerbi/realtime_retail_dashboard.pbix`.

To publish: **Publish →** your workspace. For scheduled refresh, point the
CSVs at a SharePoint/OneDrive folder and use an On-premises Data Gateway —
the export script can be re-run on a schedule to refresh the files.
