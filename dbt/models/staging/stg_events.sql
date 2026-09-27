-- dbt/models/staging/stg_events.sql
-- Raw order events -> typed, filtered staging (mirrors the Spark silver layer)
{{ config(materialized='view') }}

select
    event_id,
    cast(event_time as timestamp)      as event_time,
    order_id,
    customer_id,
    product_id,
    category,
    quantity,
    unit_price,
    discount,
    payment_method,
    device,
    region,
    round(quantity * unit_price * (1 - discount), 2) as line_total,
    date(event_time)                  as event_date
from {{ source('bronze', 'events_raw') }}
where quantity > 0
  and unit_price > 0
  and discount between 0 and 0.9
  -- must match the producer's real taxonomy (streaming/event_producer.py)
  and category in ('Electronics','Furniture','Clothing','Grocery')
