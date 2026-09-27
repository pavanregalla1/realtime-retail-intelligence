"""Build the local DuckDB warehouse that dbt builds from.

Loads:
  bronze.events_raw   <- STREAM_DIR/batch_*.jsonl  (the event stream)
  ml.anomaly_scores   <- ml/artifacts/anomaly_scores.csv (from ml/train_anomaly.py)

Output: warehouse/retail.duckdb (gitignored; reproducible via this script).

Run:  python scripts/build_warehouse.py
Then: (cd dbt && dbt build --profiles-dir .)
"""
import glob
import os
import sys

STREAM_DIR = os.environ.get("STREAM_DIR", "data/stream")
SCORES_CSV = os.environ.get("SCORES_CSV", "ml/artifacts/anomaly_scores.csv")
WAREHOUSE = os.environ.get("WAREHOUSE", "warehouse/retail.duckdb")

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is not installed (pip install dbt-duckdb)")

os.makedirs(os.path.dirname(os.path.abspath(WAREHOUSE)), exist_ok=True)
if os.path.exists(WAREHOUSE):
    os.remove(WAREHOUSE)

files = sorted(glob.glob(f"{STREAM_DIR}/batch_*.jsonl"))
if not files:
    sys.exit(f"no stream files found in {STREAM_DIR}/ — run streaming/event_producer.py first")
if not os.path.exists(SCORES_CSV):
    sys.exit(f"{SCORES_CSV} missing — run ml/train_anomaly.py first")

con = duckdb.connect(WAREHOUSE)
con.execute("CREATE SCHEMA bronze")
con.execute("CREATE SCHEMA ml")
# bulk-load the JSON-lines stream (region is NULL locally; populated by the
# prod producer — see docs/runbook.md)
con.execute("""
    CREATE TABLE bronze.events_raw AS
    SELECT event_id,
           event_time,
           order_id, customer_id, product_id, category,
           CAST(quantity AS INTEGER)   AS quantity,
           CAST(unit_price AS DOUBLE)  AS unit_price,
           CAST(discount AS DOUBLE)    AS discount,
           CAST(line_total AS DOUBLE)  AS line_total,
           payment_method, device,
           CAST(NULL AS VARCHAR)       AS region
    FROM read_json(?, format='newline_delimited')
""", [files])
n = con.execute("SELECT COUNT(*) FROM bronze.events_raw").fetchone()[0]
print(f"bronze.events_raw: {n:,} rows from {len(files)} files")

con.execute("""
    CREATE TABLE ml.anomaly_scores AS
    SELECT event_id,
           CAST(is_anomaly AS BOOLEAN) AS is_anomaly,
           anomaly_detectors
    FROM read_csv(?, header=true)
""", [SCORES_CSV])
scored = con.execute("SELECT COUNT(*) FROM ml.anomaly_scores").fetchone()[0]
print(f"ml.anomaly_scores: {scored:,} rows")
con.close()
print(f"warehouse written to {WAREHOUSE}")
