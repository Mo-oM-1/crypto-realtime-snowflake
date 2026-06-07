{{ config(materialized='table') }}

select
    symbol,
    side,
    level,
    price,
    qty,
    notional,
    last_update_id,
    ingest_time
from {{ ref('int_depth_levels') }}
