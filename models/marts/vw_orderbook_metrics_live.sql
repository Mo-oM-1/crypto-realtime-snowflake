{{ config(materialized='view') }}

with recent as (

    select
        symbol,
        side,
        price,
        qty,
        last_update_id
    from {{ ref('stg_depth_levels') }}
    where ingest_time >= dateadd('minute', -120, current_timestamp())

),

latest as (

    select *
    from recent
    qualify last_update_id = max(last_update_id) over (partition by symbol)

),

agg as (

    select
        symbol,
        max(case when side = 'bid' then price end)::number(38,8)      as best_bid,
        min(case when side = 'ask' then price end)::number(38,8)      as best_ask,
        sum(case when side = 'bid' then qty else 0 end)::number(38,8) as bid_vol,
        sum(case when side = 'ask' then qty else 0 end)::number(38,8) as ask_vol
    from latest
    group by symbol

)

select
    symbol,
    best_bid,
    best_ask,
    ((best_bid + best_ask) / 2)::number(38,8)                                       as mid,
    ((best_ask - best_bid) / nullif((best_bid + best_ask) / 2, 0) * 10000)::number(38,8) as spread_bps,
    bid_vol,
    ask_vol,
    (bid_vol / nullif(bid_vol + ask_vol, 0))::number(38,8)                           as imbalance,
    ((best_bid * ask_vol + best_ask * bid_vol) / nullif(bid_vol + ask_vol, 0))::number(38,8) as microprice
from agg
