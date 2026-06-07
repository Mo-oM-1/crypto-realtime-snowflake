-- Order-book invariants for the live snapshot:
-- best_bid <= best_ask, non-negative spread, imbalance in [0,1],
-- mid and microprice within [best_bid, best_ask] (small tolerance for rounding).
select *
from {{ ref('vw_orderbook_metrics_live') }}
where best_bid > best_ask
   or spread_bps < 0
   or imbalance < 0 or imbalance > 1
   or mid < best_bid - 1e-6 or mid > best_ask + 1e-6
   or microprice < best_bid - 1e-6 or microprice > best_ask + 1e-6
