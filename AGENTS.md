# Cortex Code — Reusable Prompts (crypto real-time)

Ces prompts pilotent l'agent **Cortex Code** (skills dans `.cortex/skills/`).
Lance-les dans Cortex Code (Snowsight, IDE desktop ou CLI), rôle `CRYPTO_PIPELINE_ROLE`.

---

## $flatten-variant

**But :** scanner les tables VARIANT brutes et générer la couche dbt staging → intermediate → marts.

**Prompt :**

```
Scan the CRYPTO schema of the RAW database and create a complete dbt project from the
RAW_TRADES and RAW_DEPTH tables.

The raw JSON is the full Binance combined-stream message, stored in the VARIANT column RECORD:
- RECORD:stream is the stream name (e.g. "btcusdt@trade" or "btcusdt@depth20@1000ms").
  Derive SYMBOL = UPPER(SPLIT_PART(RECORD:stream, '@', 1)).
- Trades (RAW_TRADES): fields are under RECORD:data
  (e=event type, E=event time ms, s=symbol, t=trade id, p=price, q=quantity,
   T=trade time ms, m=is buyer market maker). Convert ms epochs with TO_TIMESTAMP_NTZ(x, 3).
  Produce stg_trades with one row per trade, deduplicated on trade_id (QUALIFY ROW_NUMBER()).
- Depth (RAW_DEPTH): RECORD:data has lastUpdateId, bids and asks as ARRAYS of [price, qty] pairs.
  Use LATERAL FLATTEN to explode bids/asks into one row per (symbol, side, level, price, qty)
  -> model stg_depth_levels (side in {'bid','ask'}, level = array index, keep INGEST_TIME and lastUpdateId).

Generate staging (views), intermediate (tables) and marts (tables), with _sources.yml and
_schema.yml (not_null + unique on keys).

Conventions: cast every price/quantity/volume field to NUMBER(38,8) (never FLOAT); use descriptive
CTE names that don't clash with source column names (e.g. bid_levels / ask_levels) and qualify
flatten inputs (LATERAL FLATTEN(input => source.bids)).

Then run dbt compile, dbt run, and dbt test.
```

---

## $realtime-marts

**But :** générer les marts TEMPS RÉEL (vues live calculées à la lecture) + l'historique (Dynamic Tables).
À lancer **après** `$flatten-variant` (utilise `stg_trades`, `stg_depth_levels`).

**Prompt :**

```
Using ref('stg_trades') and ref('stg_depth_levels'), generate these Gold models in models/marts/.

LIVE views (each with {{ config(materialized='view') }}), filtered to the last 120 minutes for performance:

1. vw_ohlcv_1min_live — per symbol and minute = TIME_SLICE(traded_at, 1, 'MINUTE'):
   open  = MIN_BY(price, trade_id),
   close = MAX_BY(price, trade_id),
   high  = MAX(price), low = MIN(price),
   volume = SUM(quantity), quote_volume = SUM(price*quantity),
   trade_count = COUNT(*),
   vwap = SUM(price*quantity) / NULLIF(SUM(quantity), 0).

2. vw_orderbook_metrics_live — latest snapshot per symbol.
   IMPORTANT: identify the latest snapshot with last_update_id (Binance monotonic sequence),
   NOT ingest_time (ingest_time is batch-uniform here). Keep only rows of the max snapshot, e.g.
   WHERE last_update_id = MAX(last_update_id) OVER (PARTITION BY symbol), then aggregate levels:
   best_bid = MAX(price) where side='bid', best_ask = MIN(price) where side='ask',
   mid = (best_bid+best_ask)/2,
   spread_bps = (best_ask-best_bid)/mid*10000,
   bid_vol = SUM(qty where side='bid'), ask_vol = SUM(qty where side='ask'),
   imbalance = bid_vol/NULLIF(bid_vol+ask_vol,0),
   microprice = (best_bid*ask_vol + best_ask*bid_vol)/NULLIF(bid_vol+ask_vol,0).

3. vw_market_metrics_live — from vw_ohlcv_1min_live:
   price change % over 1/5/15 min, volume z-score over a rolling window (flag is_volume_anomaly when |z|>3),
   realized volatility (stddev of log returns), rsi_14.

HISTORY as Dynamic Tables (each with
   {{ config(materialized='dynamic_table', target_lag='1 minute', snowflake_warehouse='WH_CRYPTO_XS', on_configuration_change='apply') }}):

4. fct_ohlcv_1min — same OHLCV as #1 but full history (no time filter).
5. fct_orderbook_snapshots — per symbol & ingest_time: best_bid, best_ask, mid, spread_bps, imbalance.

6. dim_symbols — from the seed dim_symbols (ref('dim_symbols')).

Add _schema.yml: not_null on (symbol, minute) where relevant, and a unique combination of (symbol, minute).
Then run dbt compile, dbt run, dbt test.
```

---

### Règles de gouvernance (anti « vibe coding »)

- **Revue humaine** systématique du SQL généré (diff Git) avant merge.
- Les **tests dbt** (not_null/unique) servent de filet — `dbt test` doit être vert.
- Modèles **idempotents** ; respect du RBAC via le rôle `CRYPTO_PIPELINE_ROLE`.
- Respecter la chaîne `sources → staging → intermediate → marts`.
