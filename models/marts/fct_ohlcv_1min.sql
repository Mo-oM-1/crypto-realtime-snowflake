{{ config(
    materialized='incremental',
    unique_key=['symbol', 'minute'],
    incremental_strategy='merge'
) }}

-- Historique OHLCV à la minute.
-- INCREMENTAL (pas Dynamic Table) : open/close utilisent min_by/max_by, qui ne sont PAS
-- incrémentalement maintenables -> une DT recalculerait tout l'historique à chaque target_lag
-- (poste de coût structurel). Ici on ne (re)calcule que les minutes récentes, puis MERGE
-- sur (symbol, minute) : la minute en cours se met à jour, les minutes closes sont figées.

with trades as (

    select
        symbol,
        time_slice(traded_at, 1, 'MINUTE') as minute,
        trade_id,
        price,
        quantity
    from {{ ref('stg_trades') }}

    {% if is_incremental() %}
    -- Append-only : retraiter seulement depuis la dernière minute connue (- 5 min de marge
    -- pour la minute encore ouverte et d'éventuels trades tardifs).
    where traded_at >= (
        select coalesce(dateadd('minute', -5, max(minute)), '1970-01-01'::timestamp_ntz)
        from {{ this }}
    )
    {% endif %}

)

{{ ohlcv_select('trades') }}
