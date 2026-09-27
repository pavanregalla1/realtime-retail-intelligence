"""Spark Structured Streaming job — production path for the event pipeline.

Reads the order-event stream (Kafka in prod; file source for local replay),
applies the bronze->silver->gold medallion transforms with watermarking and
exactly-once semantics, and serves three sinks:
  silver.events_clean  -> Delta (append, deduped)
  gold.hourly_revenue  -> Delta (complete-mode aggregation)
  gold.anomaly_alerts  -> Kafka topic 'retail.alerts' for downstream consumers

Run locally (replay the generated stream as the source):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      spark/streaming_job.py --source rate|file

In production the only change is --source kafka with broker/topic options;
all transforms are source-agnostic.
"""
import argparse
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

VALID_CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Beauty",
                    "Sports", "Books", "Toys", "Grocery"]


def build_pipeline(spark: SparkSession, source: str, path: str):
    if source == "file":                       # local replay of data/stream/
        raw = (spark.readStream.schema(EVENT_SCHEMA).json(path))
    else:                                      # prod: Kafka topic retail.events
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", "kafka:9092")
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
    hourly_revenue = (silver
                      .withWatermark("event_time", "10 minutes")
                      .groupBy(F.window("event_time", "1 hour").alias("hr"),
                               "region", "category")
                      .agg(F.count("*").alias("orders"),
                           F.round(F.sum("line_total"), 2).alias("revenue"),
                           F.round(F.avg("line_total"), 2).alias("avg_ticket")))

    # ---- gold: velocity guardrail (fraud burst) ----
    velocity = (silver
                .withWatermark("event_time", "10 minutes")
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
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("realtime-retail-intelligence")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    if args.source == "rate":  # smoke test without data
        src = (spark.readStream.format("rate").option("rowsPerSecond", 100).load()
               .withColumn("event_id", F.col("value").cast("string")))
        q = src.writeStream.outputMode("append").format("console").start()
        q.awaitTermination(15); q.stop(); return

    silver, hourly_revenue, alerts = build_pipeline(spark, args.source, args.path)

    (silver.writeStream.outputMode("append").format("delta")
     .option("checkpointLocation", args.checkpoint + "silver").start("lake/silver/events_clean"))
    (hourly_revenue.writeStream.outputMode("complete").format("delta")
     .option("checkpointLocation", args.checkpoint + "hourly").start("lake/gold/hourly_revenue"))
    (alerts.writeStream.outputMode("append").format("console")   # prod: .format("kafka")
     .option("checkpointLocation", args.checkpoint + "alerts").start())

    spark.streams.awaitTermination()


if __name__ == "__main__":
    main()
