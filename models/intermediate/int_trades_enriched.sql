{{ config(materialized='table') }}

with trades as (

    select * from {{ ref('stg_trades') }}

)

select
    trade_uid,
    symbol,
    trade_id,
    price,
    quantity,
    price * quantity                                as trade_value,
    case when is_buyer_market_maker
         then 'sell' else 'buy' end                 as taker_side,
    traded_at,
    event_time,
    is_buyer_market_maker,
    ingest_time
from trades
