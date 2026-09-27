# Interview Talk Track — Real-Time Retail Intelligence (5 minutes)

## The one-liner
"I built a real-time retail data platform: order events stream through Kafka into
Spark Structured Streaming, ML models score the stream for fraud and price
anomalies, and a Power BI dashboard design shows revenue with inline alerting."

## Minute 1 — The problem (business framing)
"Retailers lose money two ways: fraud they catch too late, and pricing glitches
that sell products for a penny for hours before anyone notices. Batch pipelines
that run nightly mean you find out the next morning. I wanted a system that
catches both *while they're happening*."

## Minute 2 — Architecture (draw it on the whiteboard)
"Events land in Kafka — that decouples producers from processing and gives me
replay for free. Spark Structured Streaming runs the medallion pipeline with a
10-minute watermark so late mobile retries still land in the right window, and
checkpointing gives exactly-once. Gold layer splits three ways: Delta tables for
BI, an alerts topic for downstream consumers, and dbt marts for analysts."

## Minute 3 — The ML (this is the differentiator)
"Here's the key design decision: ML runs as a *sidecar*, not inline. The
streaming job runs deterministic transforms plus a velocity guardrail; the
models train and score in batch and write flags to `anomaly_scores.csv`.
But fraud can't wait for a batch — so a deterministic velocity guardrail runs
inline: 10+ orders from one customer in 30 minutes raises a `FRAUD_VELOCITY`
alert in the streaming job itself.
The anomaly system is three detectors: a customer-window IsolationForest for
collective fraud, a product-window IsolationForest for price glitches, and an
event-level model for point anomalies. Any detector firing raises an alert.
On my synthetic test stream it caught the injected fraud burst and the price
glitch at 100% recall, with a 3.02% overall alert rate."

## Minute 4 — The forecast
"Separately I built an hourly revenue forecaster — lag features, rolling
statistics, calendar features, GradientBoosting — backtested on the last 24
hours of the synthetic stream. It came in 83.4% lower MAE than a naive-mean
baseline. That feeds a forecast-vs-actual page in the dashboard design with a
promo-uplift what-if slider for the business team."

## Minute 5 — Scale + close
"The demo is 80k events on a single machine; the design scales by adding Kafka
partitions and Spark executors — I haven't load-tested 10M+/day, so I present
that as the design direction, not a measured result. If they ask about cloud:
same shape runs serverless on Event Hubs + Stream Analytics + Databricks SQL.
The whole thing is on my GitHub — pipeline, models, dbt, dashboard build guide."

## Questions you'll get (and answers)
- *"Why not score inline in Spark?"* → Latency budget. ML inference per event
  adds tail latency; batch scoring is plenty for pricing glitches, and true
  emergencies are covered by the inline velocity rule.
- *"How do you handle late data?"* → 10-min watermark; older events are dropped
  (bounded state vs. completeness trade-off).
- *"False positives?"* → 3.02% overall alert rate on the synthetic stream;
  alert thresholds tunable per detector; the guardrail is deliberately strict
  because fraud cost >> alert cost.
- *"How would you deploy the model?"* → Serialize with joblib, version in the
  artifact store, sidecar container pulls the pinned version; rollback = repin.
