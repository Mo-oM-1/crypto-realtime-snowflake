{{ config(materialized='view') }}

with trades as (

    select
        symbol,
        time_slice(traded_at, 1, 'MINUTE') as minute,
        trade_id,
        price,
        quantity
    from {{ ref('stg_trades') }}
    -- Fenêtre sur traded_at = EVENT-TIME (horodatage du trade côté Binance).
    -- sysdate() renvoie l'heure UTC en TIMESTAMP_NTZ, cohérent avec traded_at (NTZ UTC) ;
    -- current_timestamp() est LTZ et décalerait la fenêtre selon le fuseau de la session.
    where traded_at >= dateadd('minute', -120, sysdate())

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
