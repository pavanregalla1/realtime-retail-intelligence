# Exactly-once handling

How this pipeline avoids double-counting and lost events. (Applies to the
production Kafka topology; the local file-replay mode inherits the same
guarantees from the same code path.)

## 1. Kafka offset management via Spark checkpoints

The Kafka source is created with `.option("startingOffsets", ...)` (default
`latest`; `--starting-offsets earliest` when the topic was populated before
the job started, e.g. `run.sh`'s Kafka mode) and a `checkpointLocation` per
query (`spark/checkpoints/<query>` for file replay, `spark/checkpoints-kafka/<query>`
for the Kafka path). Spark's Kafka source commits offsets to the checkpoint
**only after the micro-batch's sink write succeeds**. On restart the query resumes
from the last committed offset — no replay of already-written batches, no skipped batches.

To reprocess history deliberately (backfill), delete the checkpoint directory
and set `startingOffsets` to `earliest`.

## 2. Spark checkpointing and write-ahead log

Every streaming query sets `checkpointLocation`:

- `spark/checkpoints/silver`, `.../hourly`, `.../alerts`

The checkpoint stores (a) source offsets, (b) the write-ahead log of the
sink, and (c) state for stateful operators (watermark, deduplication state,
window aggregates). If the driver crashes mid-batch, the batch is replayed
from the source and the sink write is retried — the *sink* is what makes this
exactly-once rather than at-least-once (see below).

## 3. Idempotent sinks — dedupe on `event_id`

- **Silver** (`dropDuplicates(["event_id"])` with a 10-minute watermark):
  retried batches cannot insert the same event twice. `event_id` is the
  deterministic merge key — a UUID assigned once by the producer.
- **Gold hourly revenue** runs in **complete mode**: each trigger rewrites the
  full aggregation result for the watched windows, so replays converge to the
  same numbers instead of adding duplicates.
- **dbt marts** (`fct_orders`) are `incremental` with `unique_key='event_id'`,
  i.e. a merge on the same deterministic key.
- **Alerts** carry `(window_start, customer_id, alert_type)` — downstream
  consumers dedupe on that triple.

## 4. Replay semantics

`startingOffsets=earliest` + a fresh checkpoint reproduces the full history
deterministically: the transforms are pure functions of the ordered event log
and watermarks are event-time based (not wall-clock). Deleting
`spark/checkpoints/` and `lake/` and re-running on the same input files
reproduces identical row counts and aggregates — note `ingest_ts` is
`current_timestamp()`, so it (and only it) differs between runs.

## 5. What exactly-once does NOT cover here

- The **file-replay source** has no offsets — "exactly-once" there means the
  file listing + idempotent sinks; moving a file mid-run can double-read it.
- The **console alert sink** in local runs is not transactional; the Kafka
  alert sink (`ALERTS_SINK=kafka`) inherits Kafka's idempotent producer +
  checkpointed offsets.
- Late events beyond the 10-minute watermark are dropped (documented
  trade-off: bounded state vs. completeness).
