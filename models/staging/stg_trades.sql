{{ config(materialized='view') }}

with source as (

    select
        record,
        ingest_time
    from {{ source('raw', 'raw_trades') }}

),

flattened as (

    select
        upper(split_part(record:stream::string, '@', 1))      as symbol,
        record:data:t::number                                 as trade_id,
        record:data:p::number(38,8)                           as price,
        record:data:q::number(38,8)                           as quantity,
        to_timestamp_ntz(record:data:T::number, 3)            as traded_at,
        to_timestamp_ntz(record:data:E::number, 3)            as event_time,
        record:data:m::boolean                                as is_buyer_market_maker,
        ingest_time
    from source

)

select
    symbol || '-' || trade_id::string as trade_uid,
    symbol,
    trade_id,
    price,
    quantity,
    traded_at,
    event_time,
    is_buyer_market_maker,
    ingest_time
from flattened
qualify row_number() over (
    partition by symbol, trade_id
    order by ingest_time
) = 1
