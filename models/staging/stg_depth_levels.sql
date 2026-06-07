{{ config(materialized='view') }}

with source as (

    select
        upper(split_part(record:stream::string, '@', 1)) as symbol,
        record:data:lastUpdateId::number                 as last_update_id,
        record:data:bids                                 as bids,
        record:data:asks                                 as asks,
        ingest_time
    from {{ source('raw', 'raw_depth') }}

),

bid_levels as (

    select
        source.symbol,
        source.last_update_id,
        source.ingest_time,
        'bid'                       as side,
        f.index                     as level,
        f.value[0]::number(38,8)    as price,
        f.value[1]::number(38,8)    as qty
    from source,
         lateral flatten(input => source.bids) f

),

ask_levels as (

    select
        source.symbol,
        source.last_update_id,
        source.ingest_time,
        'ask'                       as side,
        f.index                     as level,
        f.value[0]::number(38,8)    as price,
        f.value[1]::number(38,8)    as qty
    from source,
         lateral flatten(input => source.asks) f

)

select symbol, side, level, price, qty, last_update_id, ingest_time from bid_levels
union all
select symbol, side, level, price, qty, last_update_id, ingest_time from ask_levels
