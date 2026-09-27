-- dbt/models/marts/fct_orders.sql
-- One row per order event, enriched with anomaly flags from the ML layer
{{ config(materialized='incremental', unique_key='event_id') }}

select
    s.*,
    coalesce(a.is_anomaly, false)       as is_anomaly,
    a.anomaly_detectors                 -- array: which of the 3 detectors fired
from {{ ref('stg_events') }} s
left join {{ source('ml', 'anomaly_scores') }} a
    on s.event_id = a.event_id

{% if is_incremental() %}
where s.event_time > (select max(event_time) from {{ this }})
{% endif %}
