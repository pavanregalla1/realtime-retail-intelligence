"""Spark Structured Streaming job — production path for the event pipeline.

Reads the order-event stream (Kafka in prod; file source for local replay),
applies the bronze->silver->gold medallion transforms with watermarking and
exactly-once semantics, and serves three sinks:
  silver.events_clean  -> Delta (append, deduped)
  gold.hourly_revenue  -> Delta (complete-mode aggregation)
  gold.anomaly_alerts  -> Kafka topic 'retail.alerts' for downstream consumers

Run locally (replay the generated stream as the source):
  python spark/streaming_job.py --source file --sink-format parquet --timeout 120

In production the only change is --source kafka with broker/topic options;
all transforms are source-agnostic. The kafka source needs the Kafka package
for Spark — match its Spark/Scala version to your pyspark, e.g.:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \\
      spark/streaming_job.py --source kafka

Env:
  ALERTS_SINK=kafka   publish velocity alerts to the retail.alerts Kafka topic
                      (default: console)
  KAFKA_BROKERS       bootstrap servers for the kafka source/sink
                      (default: kafka:9092 for source, localhost:9092 for sink)
"""
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", T.DoubleType()),
    T.StructField("discount", T.DoubleType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("region", T.StringType()),
])

# Must match the producer's real taxonomy (streaming/event_producer.py).
# A previous version listed a different set here and silently dropped most events.
VALID_CATEGORIES = ["Electronics", "Furniture", "Clothing", "Grocery"]


def build_pipeline(spark: SparkSession, source: str, path: str, trigger_files: int = 0):
    if source == "file":                       # local replay of data/stream/
        reader = spark.readStream.schema(EVENT_SCHEMA)
        if trigger_files and trigger_files > 0:
            reader = reader.option("maxFilesPerTrigger", trigger_files)
        raw = reader.json(path)
    else:                                      # prod: Kafka topic retail.events
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers",
                       os.environ.get("KAFKA_BROKERS", "kafka:9092"))
               .option("subscribe", "retail.events")
               .option("startingOffsets", "latest").load()
               .select(F.from_json(F.col("value").cast("string"),
                                   EVENT_SCHEMA).alias("e")).select("e.*"))

    bronze = raw.withColumn("ingest_ts", F.current_timestamp())

    # ---- silver: quarantine bad rows, dedupe ----
    silver = (bronze
              .withWatermark("event_time", "10 minutes")
              .dropDuplicates(["event_id"])
              .filter(F.col("quantity") > 0)
              .filter(F.col("unit_price") > 0)
              .filter(F.col("discount").between(0, 0.9))
              .filter(F.col("category").isin(VALID_CATEGORIES))
              .withColumn("line_total",
                          F.round(F.col("quantity") * F.col("unit_price")
                                  * (1 - F.col("discount")), 2))
              .withColumn("event_date", F.to_date("event_time")))

    # ---- gold: hourly revenue per region/category ----
    # (silver already carries the 10-minute event_time watermark — Spark 4.x
    #  forbids redefining it, so the aggregations reuse it directly)
    hourly_revenue = (silver
                      .groupBy(F.window("event_time", "1 hour").alias("hr"),
                               "region", "category")
                      .agg(F.count("*").alias("orders"),
                           F.round(F.sum("line_total"), 2).alias("revenue"),
                           F.round(F.avg("line_total"), 2).alias("avg_ticket")))

    # ---- gold: velocity guardrail (fraud burst) ----
    velocity = (silver
                .groupBy(F.window("event_time", "30 minutes").alias("w"),
                         "customer_id")
                .agg(F.count("*").alias("orders_30min"),
                     F.round(F.sum("line_total"), 2).alias("spend_30min")))
    alerts = (velocity.filter(F.col("orders_30min") >= 10)
              .select(F.col("w.start").alias("window_start"),
                      "customer_id", "orders_30min", "spend_30min",
                      F.lit("FRAUD_VELOCITY").alias("alert_type"),
                      F.current_timestamp().alias("alert_ts")))

    return silver, hourly_revenue, alerts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="file", choices=["file", "kafka", "rate"])
    ap.add_argument("--path", default="data/stream/")
    ap.add_argument("--checkpoint", default="spark/checkpoints/")
    ap.add_argument("--sink-format", default="delta",
                    help="sink for silver/gold tables: delta (prod) or parquet (smoke tests)")
    ap.add_argument("--timeout", type=int, default=0,
                    help="stop the streaming queries after N seconds (0 = run forever)")
    ap.add_argument("--trigger-files", type=int, default=0,
                    help="max files per micro-batch for the file source (0 = unlimited)")
    ap.add_argument("--progress-log", default="",
                    help="append per-micro-batch stats (query, batch_id, duration_ms,"
                         " input_rows) as JSON lines to this file — polled from the"
                         " driver via query.lastProgress (no listener callbacks)")
    ap.add_argument("--poll-secs", type=int, default=5,
                    help="polling interval for --progress-log")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("realtime-retail-intelligence")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    if args.source == "rate":  # smoke test without data
        src = (spark.readStream.format("rate").option("rowsPerSecond", 100).load()
               .withColumn("event_id", F.col("value").cast("string")))
        q = src.writeStream.outputMode("append").format("console").start()
        q.awaitTermination(15); q.stop(); return

    silver, hourly_revenue, alerts = build_pipeline(spark, args.source, args.path,
                                                     args.trigger_files)

    alerts_sink = os.environ.get("ALERTS_SINK", "console")  # console | kafka
    if alerts_sink == "kafka":
        alerts_out = (alerts.select(F.to_json(F.struct("*")).alias("value"))
                      .writeStream.outputMode("append").format("kafka")
                      .option("kafka.bootstrap.servers",
                              os.environ.get("KAFKA_BROKERS", "localhost:9092"))
                      .option("topic", "retail.alerts"))
    else:
        alerts_out = alerts.writeStream.outputMode("append").format("console")

    # complete mode needs Delta; the parquet smoke path uses append mode on the
    # watermarked aggregation (each hour-window is emitted once — same rows)
    gold_mode = "complete" if args.sink_format == "delta" else "append"

    silver_q = (silver.writeStream.outputMode("append").format(args.sink_format)
                .queryName("silver")
                .option("checkpointLocation", args.checkpoint + "silver")
                .start("lake/silver/events_clean"))
    hourly_q = (hourly_revenue.writeStream.outputMode(gold_mode).format(args.sink_format)
                .queryName("hourly_revenue")
                .option("checkpointLocation", args.checkpoint + "hourly")
                .start("lake/gold/hourly_revenue"))
    alerts_q = (alerts_out.queryName("alerts")
                .option("checkpointLocation", args.checkpoint + "alerts").start())

    if args.timeout and args.timeout > 0:
        import time as _time
        import json as _json
        queries = {"silver": silver_q, "hourly_revenue": hourly_q, "alerts": alerts_q}
        if args.progress_log:
            open(args.progress_log, "w").close()  # truncate from previous runs
        seen, deadline = set(), _time.time() + args.timeout
        while _time.time() < deadline and any(q.isActive for q in queries.values()):
            _time.sleep(args.poll_secs)
            for name, q in queries.items():
                try:
                    p = q.lastProgress
                except Exception:
                    continue
                if p and (name, p["batchId"]) not in seen:
                    seen.add((name, p["batchId"]))
                    if args.progress_log:
                        with open(args.progress_log, "a") as fh:
                            fh.write(_json.dumps({
                                "query": name, "batch_id": p["batchId"],
                                "duration_ms": (p.get("durationMs") or {})
                                               .get("triggerExecution", 0),
                                "input_rows": p.get("numInputRows", 0)}) + "\n")
        spark.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
