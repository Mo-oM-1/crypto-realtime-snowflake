{{ config(
    materialized='dynamic_table',
    target_lag='5 minute',
    snowflake_warehouse='WH_CRYPTO_XS',
    on_configuration_change='apply'
) }}

with agg as (

    select
        symbol,
        last_update_id,
        max(ingest_time)                                             as ingest_time,
        max(case when side = 'bid' then price end)::number(38,8)      as best_bid,
        min(case when side = 'ask' then price end)::number(38,8)      as best_ask,
        sum(case when side = 'bid' then qty else 0 end)::number(38,8) as bid_vol,
        sum(case when side = 'ask' then qty else 0 end)::number(38,8) as ask_vol
    from {{ ref('stg_depth_levels') }}
    group by symbol, last_update_id

)

select
    symbol,
    last_update_id,
    ingest_time,
    best_bid,
    best_ask,
    ((best_bid + best_ask) / 2)::number(38,8)                                            as mid,
    ((best_ask - best_bid) / nullif((best_bid + best_ask) / 2, 0) * 10000)::number(38,8) as spread_bps,
    (bid_vol / nullif(bid_vol + ask_vol, 0))::number(38,8)                               as imbalance
from agg
