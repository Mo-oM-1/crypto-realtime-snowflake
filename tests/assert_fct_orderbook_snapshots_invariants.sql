-- Order-book invariants for the full-history Dynamic Table:
-- best_bid <= best_ask, non-negative spread, imbalance in [0,1],
-- mid within [best_bid, best_ask] (small tolerance for rounding).
-- One row per true snapshot (symbol, last_update_id), so no crossed book
-- from mixing snapshots that share an ingest_time.
select *
from {{ ref('fct_orderbook_snapshots') }}
where best_bid > best_ask
   or spread_bps < 0
   or imbalance < 0 or imbalance > 1
   or mid < best_bid - 1e-6 or mid > best_ask + 1e-6
