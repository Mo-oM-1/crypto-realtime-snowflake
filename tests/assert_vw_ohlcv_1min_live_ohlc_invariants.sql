-- OHLC invariants: high is the max, low is the min, all prices positive,
-- volume non-negative, vwap within [low, high] (small tolerance for rounding).
select *
from {{ ref('vw_ohlcv_1min_live') }}
where high < low
   or high < open
   or high < close
   or low > open
   or low > close
   or open <= 0 or high <= 0 or low <= 0 or close <= 0
   or volume < 0
   or vwap < low - 1e-6
   or vwap > high + 1e-6
