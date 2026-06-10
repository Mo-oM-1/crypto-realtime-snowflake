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
    where traded_at >= dateadd('minute', -{{ var('live_window_minutes', 120) }}, sysdate())

)

{{ ohlcv_select('trades') }}
