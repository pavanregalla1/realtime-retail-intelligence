# Infrastructure & Architecture

## Data flow
```
order events (web/app/POS)
        │  JSON, ~8/min avg (~10× during flash sales)
        ▼
Kafka topic: retail.events  (3 partitions, 7-day retention)
        │
        ▼
Spark Structured Streaming  (bronze → silver → gold, watermark 10 min,
                             exactly-once via checkpointing + Delta)
        ├── silver.events_clean      (Delta, append, deduped on event_id)
        ├── gold.hourly_revenue      (Delta, complete mode)
        └── gold.anomaly_alerts      → Kafka topic retail.alerts
                                            │
        ┌───────────────────────────────────┼──────────────────┐
        ▼                                   ▼                  ▼
Power BI (dashboard design          dbt marts               ML sidecar
+ build guide;                       (fct_orders,             (batch training +
 DirectQuery-ready)                  agg_hourly_revenue)      scoring →
                                                             anomaly_scores.csv)
```

## Why this shape
- **Kafka in front of Spark:** decouples producers from processing; replayable
  when the streaming job is redeployed (no data loss on restarts).
- **Watermark 10 min:** late events (mobile retries) still land in the right
  window; older than 10 min is dropped by the watermark (bounded state vs.
  completeness trade-off).
- **Delta Lake:** ACID merges for the incremental `fct_orders` dbt model,
  time travel for debugging a bad deploy.
- **ML as a sidecar, not inline:** the streaming job runs only deterministic
  transforms plus the velocity guardrail; the IsolationForest/GradientBoosting
  models train and score in batch (`ml/`), writing flags to
  `anomaly_scores.csv`. Deterministic guardrails (velocity rule) run inline
  for near-real-time fraud alerts.
- **dbt after gold:** analysts get tested, documented marts without touching
  the streaming job.

## Scale notes
- Current demo: 80,628 events / 7 days on a single machine. The design scales
  by adding Kafka partitions + Spark executors (partition key: `customer_id`
  for the velocity aggregate keeps state local) — but 10M+/day throughput has
  **not** been load-tested; treat it as a design direction, not a measured
  result.
- State store: Spark's default state store on the checkpoint volume for the
  30-min windowed aggregates (RocksDB can be enabled via
  `spark.sql.streaming.stateStore.provider` for larger state).

## Cost-conscious alternative (for interviews)
Same pipeline runs serverless: Event Hubs → Azure Stream Analytics →
Synapse/Databricks SQL → Power BI. Mention when the interviewer asks about cloud.
