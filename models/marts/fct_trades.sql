{{ config(materialized='table') }}

select
    trade_uid,
    symbol,
    trade_id,
    price,
    quantity,
    trade_value,
    taker_side,
    traded_at,
    event_time,
    is_buyer_market_maker,
    ingest_time
from {{ ref('int_trades_enriched') }}
