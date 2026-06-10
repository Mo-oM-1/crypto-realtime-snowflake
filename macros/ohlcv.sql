{#
  Agregation OHLCV 1 min, partagee entre vw_ohlcv_1min_live (live) et fct_ohlcv_1min
  (historique). Source = un CTE/relation exposant symbol, minute, trade_id, price, quantity.
  open  = premier trade de la minute (min trade_id), close = dernier (max trade_id).
#}
{% macro ohlcv_select(source) %}
select
    symbol,
    minute,
    min_by(price, trade_id)::number(38,8)                            as open,
    max_by(price, trade_id)::number(38,8)                            as close,
    max(price)::number(38,8)                                         as high,
    min(price)::number(38,8)                                         as low,
    sum(quantity)::number(38,8)                                      as volume,
    sum(price * quantity)::number(38,8)                              as quote_volume,
    count(*)                                                         as trade_count,
    (sum(price * quantity) / nullif(sum(quantity), 0))::number(38,8) as vwap
from {{ source }}
group by symbol, minute
{% endmacro %}
