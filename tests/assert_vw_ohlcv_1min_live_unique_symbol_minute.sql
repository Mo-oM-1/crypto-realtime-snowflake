-- Asserts that vw_ohlcv_1min_live has a unique combination of (symbol, minute).
select
    symbol,
    minute,
    count(*) as n_rows
from {{ ref('vw_ohlcv_1min_live') }}
group by symbol, minute
having count(*) > 1
