"""Simulated real-time order event stream.

Writes micro-batch JSON files to data/stream/ (one file = one micro-batch),
exactly as a Kafka/Kinesis consumer would see them. Three anomalies are
injected on purpose for the ML layer to catch:
  1. Flash-sale spike  (day 3, 10x volume for 2h)
  2. Fraud-like burst  (day 5, one customer, 40 high-value orders in 20 min)
  3. Price glitch      (day 6, one product priced at $0.01 for 30 min)

PRODUCTION PATH: set STREAM_BACKEND=kafka|kinesis and the same events go to
Kafka topic / Kinesis stream via the producers below (credentials via env).
"""
import json, os, random, uuid
from datetime import datetime, timedelta

random.seed(7)
OUT = "data/stream"
os.makedirs(OUT, exist_ok=True)

BACKEND = os.environ.get("STREAM_BACKEND", "local")  # local | kafka | kinesis

CATEGORIES = {"Electronics": (50, 1200), "Furniture": (80, 900),
              "Clothing": (15, 150), "Grocery": (5, 40)}
PRODUCTS = [(f"P{i:04d}", cat, random.choice(["Pro","Max","Lite",""]))
            for i, cat in enumerate(
                ["Electronics"]*5 + ["Furniture"]*4 + ["Clothing"]*4 + ["Grocery"]*4, 1)]
PAYMENTS = ["card", "card", "card", "upi", "wallet", "cod"]
DEVICES  = ["mobile", "mobile", "desktop", "tablet"]

def make_event(ts, customer_id=None, product=None, price_override=None):
    pid, cat, _ = product or random.choice(PRODUCTS)
    lo, hi = CATEGORIES[cat]
    price = price_override if price_override is not None else round(random.uniform(lo, hi), 2)
    qty = random.randint(1, 3)
    discount = random.choice([0, 0, 0, 0.05, 0.1])
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "order_id": f"S{random.randint(100000,999999)}",
        "customer_id": customer_id or f"C{random.randint(1,5000):05d}",
        "product_id": pid, "category": cat,
        "quantity": qty, "unit_price": price, "discount": discount,
        "line_total": round(qty * price * (1 - discount), 2),
        "payment_method": random.choice(PAYMENTS),
        "device": random.choice(DEVICES),
    }

def send_batch(batch_id, events):
    """Local backend: one JSON-lines file per micro-batch (the 'stream')."""
    if BACKEND == "local":
        with open(f"{OUT}/batch_{batch_id:04d}.jsonl", "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
    elif BACKEND == "kafka":
        from kafka import KafkaProducer  # pip install kafka-python
        p = KafkaProducer(bootstrap_servers=os.environ["KAFKA_BROKERS"],
                          value_serializer=lambda v: json.dumps(v).encode())
        for e in events:
            p.send(os.environ.get("KAFKA_TOPIC", "order_events"), e)
        p.flush()
    elif BACKEND == "kinesis":
        import boto3  # AWS creds via env / IAM role
        k = boto3.client("kinesis", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        for e in events:
            k.put_record(StreamName=os.environ["KINESIS_STREAM"],
                         Data=json.dumps(e), PartitionKey=e["customer_id"])

# ---- 7 days, 30-min micro-batches = 336 batches ----
start = datetime(2025, 12, 1)
batch, total = 0, 0
GLITCH_PID = "P0001"
for day in range(7):
    for slot in range(48):
        ts = start + timedelta(days=day, minutes=30*slot)
        # base rate follows daily seasonality (evening peak)
        rate = 220 * (1 + 0.8 * max(0, (ts.hour - 12)) / 12) if 8 <= ts.hour <= 23 else 60
        events = [make_event(ts + timedelta(seconds=random.randint(0, 1799)))
                  for _ in range(int(random.gauss(rate, rate*0.15)))]
        # 1. flash sale: day 3 (index 2), 18:00-20:00 -> 10x volume
        if day == 2 and 36 <= slot <= 39:
            events += [make_event(ts + timedelta(seconds=random.randint(0, 1799)))
                       for _ in range(int(rate * 9))]
        # 2. fraud burst: day 5, 14:00-14:20, one customer hammering high-value orders
        if day == 4 and slot == 28:
            prod = ("P0002", "Electronics", "")
            events += [make_event(ts + timedelta(seconds=i*30), customer_id="C00999",
                                  product=prod) for i in range(40)]
        # 3. price glitch: day 6, 10:00-10:30, P0001 at $0.01
        if day == 5 and slot == 20:
            events += [make_event(ts + timedelta(seconds=random.randint(0, 1799)),
                                  product=(GLITCH_PID, "Electronics", ""),
                                  price_override=0.01) for _ in range(120)]
        send_batch(batch, events)
        batch += 1; total += len(events)

print(f"batches={batch} events={total} backend={BACKEND}")
