-- dbt/models/marts/agg_hourly_revenue.sql
-- Serving layer for the real-time dashboard: hourly revenue by region/category
{{ config(materialized='table') }}

select
    date_trunc('hour', event_time) as hour,
    region,
    category,
    count(*)                      as orders,
    round(sum(line_total), 2)      as revenue,
    round(avg(line_total), 2)     as avg_ticket,
    sum(case when is_anomaly then 1 else 0 end) as anomaly_orders
from {{ ref('fct_orders') }}
group by 1, 2, 3
