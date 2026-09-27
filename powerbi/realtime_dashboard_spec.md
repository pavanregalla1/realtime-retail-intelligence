# Real-Time Retail Intelligence — Power BI Dashboard Spec

**Mode:** DirectQuery against `gold.agg_hourly_revenue` + `gold.anomaly_alerts`
(automatic page refresh every 5 min; anomaly cards push via Power Automate)

## Page 1 — Live Revenue Pulse
| Visual | Measure / field |
|---|---|
| KPI card: Revenue (last hour) | `Revenue Last Hour = CALCULATE(SUM(agg_hourly_revenue[revenue]), agg_hourly_revenue[hour] = MAX(agg_hourly_revenue[hour]))` |
| KPI card: Orders (last hour) | analogous |
| KPI card: Active anomalies | `COUNTROWS(FILTER(anomaly_alerts, anomaly_alerts[alert_ts] >= NOW() - TIME(1,0,0)))` |
| Line chart: revenue per 5-min vs 7-day average | DAX: `Revenue 5min`, `Avg 7d Same Weekday-Hour` |
| Map: revenue by region (live) | `SUM(revenue)` by region |
| Bar: top categories this hour | |

## Page 2 — Anomaly Command Center
| Visual | Purpose |
|---|---|
| Alert feed table | `window_start, customer_id/product_id, alert_type, orders_30min, spend_30min` sorted by `alert_ts` desc |
| Gauge: % of revenue from anomalous orders | `DIVIDE(SUMX(FILTER(...is_anomaly...)), [Total Revenue])` |
| Decomposition tree | drill from alert_type → category → product |
| Flash-sale overlay | shaded band where `orders` > 3× trailing average |

## Page 3 — Forecast vs Actual
| Visual | Purpose |
|---|---|
| Line: actual hourly revenue vs model forecast | forecast table from `ml.forecast_output` |
| KPI: backtest MAE vs naive | from `ml.artifacts/forecast_metrics.json` |
| What-if parameter | "Promo uplift %" slider → `Forecast * (1 + uplift)` |

## Alerts (Power Automate)
- **Fraud velocity:** when `anomaly_alerts` gets `FRAUD_VELOCITY` → Teams message + email to risk desk within 1 min.
- **Revenue drop:** when last-hour revenue < 50% of same hour last week → push notification.
