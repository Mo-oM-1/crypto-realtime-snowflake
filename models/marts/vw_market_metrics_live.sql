{{ config(materialized='view') }}

-- Métriques de marché temps réel sur la fenêtre live (120 min, héritée de vw_ohlcv_1min_live).
-- RSI : lissage de WILDER (alpha = 1/14), comme TradingView/ta.rma, et NON une SMA.
-- Wilder est récursif (chaque moyenne dépend de la précédente) -> CTE récursive ci-dessous.
-- Note : calculé sur la fenêtre live ; sur ~120 points, l'influence du seed décroît à
-- (13/14)^~105 ≈ 5e-4, donc la valeur récente converge vers le RSI "full history".

with base as (

    select
        symbol,
        minute,
        close,
        volume,
        ln(close / nullif(lag(close) over (partition by symbol order by minute), 0)) as log_return,
        close - lag(close) over (partition by symbol order by minute)                as delta,
        row_number() over (partition by symbol order by minute)                       as rn
    from {{ ref('vw_ohlcv_1min_live') }}

),

gl as (

    select
        symbol, minute, close, volume, log_return, rn,
        case when delta > 0 then delta  else 0 end as gain,
        case when delta < 0 then -delta else 0 end as loss
    from base

),

-- Seed de Wilder : SMA des 14 premières variations (rn 2..15), posée à rn = 15.
seed as (

    select
        symbol,
        15            as rn,
        avg(gain)     as avg_gain,
        avg(loss)     as avg_loss
    from gl
    where rn between 2 and 15
    group by symbol
    having count(*) >= 14

),

-- Lissage récursif de Wilder : avg = (avg_prev * 13 + valeur_courante) / 14
wilder as (

    select symbol, rn, avg_gain, avg_loss
    from seed

    union all

    select
        g.symbol,
        g.rn,
        (w.avg_gain * 13 + g.gain) / 14 as avg_gain,
        (w.avg_loss * 13 + g.loss) / 14 as avg_loss
    from wilder w
    join gl g
        on g.symbol = w.symbol
       and g.rn = w.rn + 1

),

-- Autres métriques (fonctions de fenêtre classiques)
metrics as (

    select
        symbol, minute, close, volume, rn,
        (close - lag(close, 1)  over (partition by symbol order by minute))
            / nullif(lag(close, 1)  over (partition by symbol order by minute), 0) * 100 as price_change_pct_1min,
        (close - lag(close, 5)  over (partition by symbol order by minute))
            / nullif(lag(close, 5)  over (partition by symbol order by minute), 0) * 100 as price_change_pct_5min,
        (close - lag(close, 15) over (partition by symbol order by minute))
            / nullif(lag(close, 15) over (partition by symbol order by minute), 0) * 100 as price_change_pct_15min,
        avg(volume)        over (partition by symbol order by minute rows between 29 preceding and current row) as volume_avg_30,
        stddev(volume)     over (partition by symbol order by minute rows between 29 preceding and current row) as volume_std_30,
        stddev(log_return) over (partition by symbol order by minute rows between 13 preceding and current row) as realized_volatility
    from gl

)

select
    m.symbol,
    m.minute,
    m.close,
    m.volume,
    m.price_change_pct_1min::number(38,8)  as price_change_pct_1min,
    m.price_change_pct_5min::number(38,8)  as price_change_pct_5min,
    m.price_change_pct_15min::number(38,8) as price_change_pct_15min,
    case when m.volume_std_30 is null or m.volume_std_30 = 0 then null
         else ((m.volume - m.volume_avg_30) / m.volume_std_30)::number(38,8) end as volume_zscore,
    case when m.volume_std_30 is not null and m.volume_std_30 <> 0
              and abs((m.volume - m.volume_avg_30) / m.volume_std_30) > 3
         then true else false end as is_volume_anomaly,
    m.realized_volatility::number(38,8) as realized_volatility,
    -- RSI de Wilder (null tant qu'il n'y a pas 14 variations)
    case
        when w.avg_gain is null               then null
        when w.avg_loss = 0 and w.avg_gain = 0 then 50
        when w.avg_loss = 0                    then 100
        else (100 - 100 / (1 + w.avg_gain / w.avg_loss))::number(38,8)
    end as rsi_14
from metrics m
left join wilder w
    on w.symbol = m.symbol
   and w.rn     = m.rn
