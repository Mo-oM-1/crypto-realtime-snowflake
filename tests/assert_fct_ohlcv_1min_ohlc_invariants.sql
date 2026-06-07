-- OHLC invariants for the full-history Dynamic Table.
select *
from {{ ref('fct_ohlcv_1min') }}
where high < low
   or high < open
   or high < close
   or low > open
   or low > close
   or open <= 0 or high <= 0 or low <= 0 or close <= 0
   or volume < 0
   or vwap < low - 1e-6
   or vwap > high + 1e-6
