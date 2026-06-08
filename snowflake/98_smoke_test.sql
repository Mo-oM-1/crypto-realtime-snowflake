-- =====================================================================
-- 98_smoke_test.sql — Test end-to-end du pipeline
-- Pré-requis :
--   1) consumer en route depuis ~3-5 min (données fraîches dans RAW)
--   2) un build récent : EXECUTE DBT PROJECT ANALYTICS.PUBLIC.CRYPTO_REALTIME ARGS='build';
-- En Snowsight : "Run All", puis lis chaque résultat.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- 1) BRONZE : volumes + fraîcheur (attendu : compteurs > 0, fraîcheur faible)
SELECT 'raw_trades' AS src, COUNT(*) AS n FROM RAW.CRYPTO.RAW_TRADES
UNION ALL SELECT 'raw_depth', COUNT(*) FROM RAW.CRYPTO.RAW_DEPTH;

SELECT DATEDIFF('second', MAX(ingest_time), SYSDATE()) AS trades_freshness_s
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES;

-- 2) COMPTES PAR COUCHE (cohérence staging -> marts)
SELECT 'stg_trades'          AS model, COUNT(*) AS n FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
UNION ALL SELECT 'stg_depth_levels',    COUNT(*) FROM ANALYTICS.PUBLIC_STAGING.STG_DEPTH_LEVELS
UNION ALL SELECT 'fct_trades',          COUNT(*) FROM ANALYTICS.PUBLIC_MARTS.FCT_TRADES
UNION ALL SELECT 'fct_ohlcv_1min',      COUNT(*) FROM ANALYTICS.PUBLIC_MARTS.FCT_OHLCV_1MIN
UNION ALL SELECT 'fct_depth_snapshots', COUNT(*) FROM ANALYTICS.PUBLIC_MARTS.FCT_DEPTH_SNAPSHOTS;

-- 3) VUES LIVE (temps réel) : doivent renvoyer des lignes récentes
SELECT * FROM ANALYTICS.PUBLIC_MARTS.VW_OHLCV_1MIN_LIVE ORDER BY minute DESC LIMIT 5;
SELECT * FROM ANALYTICS.PUBLIC_MARTS.VW_ORDERBOOK_METRICS_LIVE;
SELECT symbol, minute, rsi_14, price_change_pct_5min, is_volume_anomaly
FROM ANALYTICS.PUBLIC_MARTS.VW_MARKET_METRICS_LIVE
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY minute DESC) = 1;

-- 4) RSI (Wilder) : bornes [0,100] par symbole (sanity)
SELECT symbol, ROUND(MIN(rsi_14),2) AS rsi_min, ROUND(MAX(rsi_14),2) AS rsi_max
FROM ANALYTICS.PUBLIC_MARTS.VW_MARKET_METRICS_LIVE
WHERE rsi_14 IS NOT NULL
GROUP BY symbol;

-- 5) MATÉRIALISATIONS (contrôle des refactors de revue)
--    attendu : INT_* = VIEW, FCT_* = BASE TABLE
SELECT table_name, table_type
FROM ANALYTICS.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('PUBLIC_INTERMEDIATE','PUBLIC_MARTS')
  AND table_name IN ('INT_TRADES_ENRICHED','INT_DEPTH_LEVELS','FCT_TRADES','FCT_OHLCV_1MIN','FCT_DEPTH_SNAPSHOTS')
ORDER BY table_name;

-- 6) LATENCE (SLO) : event -> réception
SELECT symbol,
       ROUND(AVG(DATEDIFF('ms', traded_at, ingest_time))/1000.0, 3) AS avg_s,
       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
             ORDER BY DATEDIFF('ms', traded_at, ingest_time))/1000.0, 3) AS p95_s
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
WHERE ingest_time >= DATEADD('minute', -5, SYSDATE())
GROUP BY symbol;

-- 7) INVARIANT order book : carnet croisé (attendu : 0)
SELECT COUNT(*) AS crossed_books
FROM ANALYTICS.PUBLIC_MARTS.VW_ORDERBOOK_METRICS_LIVE
WHERE best_bid > best_ask;

-- 8) GOUVERNANCE : dérive de schéma (attendu : vide si aucune nouvelle clé)
SELECT * FROM ANALYTICS.MONITORING.VW_SCHEMA_DRIFT;
