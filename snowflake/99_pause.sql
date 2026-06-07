-- =====================================================================
-- 99_pause.sql — Résumé de l'état du pipeline + mise en veille (FinOps)
-- À lancer quand tu arrêtes de bosser/démontrer.
-- Pour réveiller : snowflake/99_resume.sql  (+ relancer le consumer).
-- En Snowsight : sélectionne tout, "Run All". Chaque bloc renvoie un résultat.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- ============================ 1) RÉSUMÉ ============================

-- Volumes bruts (Bronze)
SELECT 'raw_trades' AS source, COUNT(*) AS n FROM RAW.CRYPTO.RAW_TRADES
UNION ALL
SELECT 'raw_depth', COUNT(*) FROM RAW.CRYPTO.RAW_DEPTH;

-- Fraîcheur (secondes depuis la dernière ingestion de trades)
SELECT
    MAX(ingest_time)                                          AS last_trade_ingest,
    DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP()) AS trades_freshness_s
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES;

-- Latence d'ingestion récente (event -> réception), p95
SELECT symbol,
       ROUND(AVG(DATEDIFF('ms', traded_at, ingest_time))/1000.0, 3) AS avg_s,
       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
             ORDER BY DATEDIFF('ms', traded_at, ingest_time))/1000.0, 3) AS p95_s
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
WHERE ingest_time >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
GROUP BY symbol;

-- État des objets d'orchestration (regarde la colonne state / scheduling_state)
SHOW TASKS          IN SCHEMA ANALYTICS.MONITORING;
SHOW ALERTS         IN SCHEMA ANALYTICS.MONITORING;
SHOW DYNAMIC TABLES IN SCHEMA ANALYTICS.PUBLIC_MARTS;

-- Anomalies / dérives loggées récemment
SELECT * FROM ANALYTICS.MONITORING.pipeline_log
ORDER BY checked_at DESC LIMIT 20;

-- Coût (crédits du warehouse sur 24 h)
SELECT DATE_TRUNC('hour', start_time) AS hour, ROUND(SUM(credits_used), 4) AS credits
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
        DATE_RANGE_START => DATEADD('day', -1, CURRENT_DATE()),
        WAREHOUSE_NAME   => 'WH_CRYPTO_XS'))
GROUP BY 1 ORDER BY 1 DESC;

-- ======================= 2) MISE EN VEILLE =======================
-- (IF EXISTS = pas d'erreur si un objet n'a pas été créé ;
--  ignore un éventuel "already suspended".)
ALTER ALERT IF EXISTS ANALYTICS.MONITORING.CRYPTO_FRESHNESS_ALERT    SUSPEND;
ALTER TASK  IF EXISTS ANALYTICS.MONITORING.CRYPTO_DBT_TEST           SUSPEND;
ALTER TASK  IF EXISTS ANALYTICS.MONITORING.CRYPTO_SCHEMA_DRIFT_CHECK SUSPEND;
ALTER TASK  IF EXISTS ANALYTICS.MONITORING.CRYPTO_QUALITY_CHECK      SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS ANALYTICS.PUBLIC_MARTS.FCT_OHLCV_1MIN          SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS ANALYTICS.PUBLIC_MARTS.FCT_ORDERBOOK_SNAPSHOTS SUSPEND;

-- Confirmation
SHOW TASKS          IN SCHEMA ANALYTICS.MONITORING;
SHOW ALERTS         IN SCHEMA ANALYTICS.MONITORING;
SHOW DYNAMIC TABLES IN SCHEMA ANALYTICS.PUBLIC_MARTS;

-- NB : arrête aussi le consumer Python (Ctrl+C). Le warehouse s'auto-suspend après 60 s.
