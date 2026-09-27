# Interview Talk Track — Real-Time Retail Intelligence (5 minutes)

## The one-liner
"I built a real-time retail data platform: order events stream through Kafka into
Spark Structured Streaming, ML models score every micro-batch for fraud and price
anomalies, and a Power BI dashboard shows revenue live with sub-minute alerting."

## Minute 1 — The problem (business framing)
"Retailers lose money two ways: fraud they catch too late, and pricing glitches
that sell $999 TVs for a penny for hours before anyone notices. Batch pipelines
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
streaming job has to stay under 2 seconds, so models score micro-batches every
5 minutes and write flags back to gold. But fraud can't wait 5 minutes — so a
deterministic velocity guardrail runs inline: 10+ orders from one customer in
30 minutes pages the risk desk in under a minute.
The anomaly system is three detectors: a customer-window IsolationForest for
collective fraud, a product-window IsolationForest for price glitches, and an
event-level model for point anomalies. Any detector firing raises an alert.
On my test stream it caught the injected fraud burst and the price glitch at
100% recall with a 3% flag rate."

## Minute 4 — The forecast
"Separately I built an hourly revenue forecaster — lag features, rolling
statistics, calendar features, GradientBoosting — backtested on the last 24
hours. It beat the naive baseline by 83%. That feeds a forecast-vs-actual page
in the dashboard with a promo-uplift what-if slider for the business team."

## Minute 5 — Scale + close
"The demo is 80k events, but nothing in the design is demo-only: add Kafka
partitions and Spark executors and the same job does 10M+ events a day, no code
change. If they ask about cloud: same shape runs serverless on Event Hubs +
Stream Analytics + Databricks SQL. The whole thing is on my GitHub — pipeline,
models, dbt, dashboard spec."

## Questions you'll get (and answers)
- *"Why not score inline in Spark?"* → Latency budget. ML inference per event
  adds tail latency; 5-min micro-batch scoring is plenty for pricing glitches,
  and true emergencies are covered by the inline rule.
- *"How do you handle late data?"* → 10-min watermark; older goes to quarantine.
- *"False positives?"* → 3% flag rate; alert thresholds tunable per detector;
  the guardrail is deliberately strict because fraud cost >> alert cost.
- *"How would you deploy the model?"* → Serialize with joblib, version in the
  artifact store, sidecar container pulls the pinned version; rollback = repin.
