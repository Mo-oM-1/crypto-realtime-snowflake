-- Asserts that fct_ohlcv_1min has a unique combination of (symbol, minute).
select
    symbol,
    minute,
    count(*) as n_rows
from {{ ref('fct_ohlcv_1min') }}
group by symbol, minute
having count(*) > 1
