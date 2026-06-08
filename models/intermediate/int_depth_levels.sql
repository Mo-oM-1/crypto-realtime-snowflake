{{ config(materialized='view') }}

with depth as (

    select * from {{ ref('stg_depth_levels') }}

)

select
    symbol,
    side,
    level,
    price,
    qty,
    price * qty         as notional,
    last_update_id,
    ingest_time
from depth
