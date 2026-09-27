# Infrastructure & Architecture

## Data flow
```
order events (web/app/POS)
        │  JSON, ~240/min at peak
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
Power BI (DirectQuery,              dbt marts               ML service
5-min auto-refresh)                 (fct_orders,             (batch scoring
                                    agg_hourly_revenue)      every 5 min:
                                                             IsolationForest
                                                             + forecast model)
```

## Why this shape
- **Kafka in front of Spark:** decouples producers from processing; replayable
  when the streaming job is redeployed (no data loss on restarts).
- **Watermark 10 min:** late events (mobile retries) still land in the right
  window; older than 10 min goes to the quarantine path.
- **Delta Lake:** ACID merges for the incremental `fct_orders` dbt model,
  time travel for debugging a bad deploy.
- **ML as a sidecar, not inline:** the streaming job stays under 2s latency;
  models score micro-batches every 5 min and write flags back to gold.
  Deterministic guardrails (velocity rule) run inline for sub-minute fraud alerts.
- **dbt after gold:** analysts get tested, documented marts without touching
  the streaming job.

## Scale notes
- Current demo: 80,628 events / 7 days. The same job handles 10M+/day by
  adding Kafka partitions + Spark executors — no code change (partition key:
  `customer_id` for the velocity aggregate keeps state local).
- State store: RocksDB on checkpoint volume for the 30-min windowed aggregates.

## Cost-conscious alternative (for interviews)
Same pipeline runs serverless: Event Hubs → Azure Stream Analytics →
Synapse/Databricks SQL → Power BI. Mention when the interviewer asks about cloud.
