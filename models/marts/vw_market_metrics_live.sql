{{ config(materialized='view') }}

with base as (

    select
        symbol,
        minute,
        close,
        volume,
        ln(close / nullif(lag(close) over (partition by symbol order by minute), 0)) as log_return
    from {{ ref('vw_ohlcv_1min_live') }}

),

windowed as (

    select
        symbol,
        minute,
        close,
        volume,
        log_return,
        (close - lag(close, 1)  over (partition by symbol order by minute))
            / nullif(lag(close, 1)  over (partition by symbol order by minute), 0) * 100 as price_change_pct_1min,
        (close - lag(close, 5)  over (partition by symbol order by minute))
            / nullif(lag(close, 5)  over (partition by symbol order by minute), 0) * 100 as price_change_pct_5min,
        (close - lag(close, 15) over (partition by symbol order by minute))
            / nullif(lag(close, 15) over (partition by symbol order by minute), 0) * 100 as price_change_pct_15min,
        avg(volume)     over (partition by symbol order by minute rows between 29 preceding and current row) as volume_avg_30,
        stddev(volume)  over (partition by symbol order by minute rows between 29 preceding and current row) as volume_std_30,
        stddev(log_return) over (partition by symbol order by minute rows between 13 preceding and current row) as realized_volatility
    from base

),

gains_losses as (

    select
        *,
        case when (close - lag(close) over (partition by symbol order by minute)) > 0
             then (close - lag(close) over (partition by symbol order by minute)) else 0 end as gain,
        case when (close - lag(close) over (partition by symbol order by minute)) < 0
             then abs(close - lag(close) over (partition by symbol order by minute)) else 0 end as loss
    from windowed

),

rsi as (

    select
        *,
        avg(gain) over (partition by symbol order by minute rows between 13 preceding and current row) as avg_gain_14,
        avg(loss) over (partition by symbol order by minute rows between 13 preceding and current row) as avg_loss_14
    from gains_losses

)

select
    symbol,
    minute,
    close,
    volume,
    price_change_pct_1min::number(38,8)  as price_change_pct_1min,
    price_change_pct_5min::number(38,8)  as price_change_pct_5min,
    price_change_pct_15min::number(38,8) as price_change_pct_15min,
    case when volume_std_30 is null or volume_std_30 = 0 then null
         else ((volume - volume_avg_30) / volume_std_30)::number(38,8) end as volume_zscore,
    case when volume_std_30 is not null and volume_std_30 <> 0
              and abs((volume - volume_avg_30) / volume_std_30) > 3
         then true else false end as is_volume_anomaly,
    realized_volatility::number(38,8) as realized_volatility,
    (100 - (100 / (1 + (avg_gain_14 / nullif(avg_loss_14, 0)))))::number(38,8) as rsi_14
from rsi
