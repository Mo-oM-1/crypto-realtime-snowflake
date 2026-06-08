{{ config(
    materialized='incremental',
    unique_key='trade_uid',
    incremental_strategy='merge'
) }}

-- Fait des trades, historique complet. INCREMENTAL : append des nouveaux trades, merge sur
-- trade_uid (idempotent -> pas de doublon). L'enrichissement vient de int_trades_enriched (vue).

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

{% if is_incremental() %}
-- Append-only : ne reprendre que les trades récents (lookback sur ingest_time) ; le merge
-- sur trade_uid garantit l'idempotence.
where ingest_time >= (
    select coalesce(dateadd('minute', -5, max(ingest_time)), '1970-01-01'::timestamp_ntz)
    from {{ this }}
)
{% endif %}
