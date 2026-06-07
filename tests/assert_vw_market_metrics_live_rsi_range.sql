-- RSI is bounded to [0, 100] by construction.
select *
from {{ ref('vw_market_metrics_live') }}
where rsi_14 < 0
   or rsi_14 > 100
