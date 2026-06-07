{{ config(
    materialized='dynamic_table',
    target_lag='1 minute',
    snowflake_warehouse='WH_CRYPTO_XS',
    on_configuration_change='apply'
) }}

with trades as (

    select
        symbol,
        time_slice(traded_at, 1, 'MINUTE') as minute,
        trade_id,
        price,
        quantity
    from {{ ref('stg_trades') }}

)

select
    symbol,
    minute,
    min_by(price, trade_id)::number(38,8)                            as open,
    max_by(price, trade_id)::number(38,8)                            as close,
    max(price)::number(38,8)                                         as high,
    min(price)::number(38,8)                                         as low,
    sum(quantity)::number(38,8)                                      as volume,
    sum(price * quantity)::number(38,8)                              as quote_volume,
    count(*)                                                         as trade_count,
    (sum(price * quantity) / nullif(sum(quantity), 0))::number(38,8) as vwap
from trades
group by symbol, minute
