{{ config(
    materialized='incremental',
    unique_key=['symbol', 'last_update_id', 'side', 'level'],
    incremental_strategy='merge'
) }}

-- Fait order book au grain (symbol, last_update_id, side, level), historique complet.
-- INCREMENTAL : append des nouveaux niveaux, merge sur la clé du snapshot (idempotent).
-- L'enrichissement (notional) vient de int_depth_levels (vue).

select
    symbol,
    side,
    level,
    price,
    qty,
    notional,
    last_update_id,
    ingest_time
from {{ ref('int_depth_levels') }}

{% if is_incremental() %}
-- Append-only : ne reprendre que les snapshots récents (lookback sur ingest_time) ; le merge
-- sur (symbol, last_update_id, side, level) garantit l'idempotence.
where ingest_time >= (
    select coalesce(dateadd('minute', -5, max(ingest_time)), '1970-01-01'::timestamp_ntz)
    from {{ this }}
)
{% endif %}
