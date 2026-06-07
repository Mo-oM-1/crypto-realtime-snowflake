-- =====================================================================
-- 02_observability.sql — SLO & monitoring du pipeline crypto temps réel
-- Rôle conseillé : CRYPTO_PIPELINE_ROLE (sauf §6 coût : ACCOUNTADMIN possible).
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- 1) LATENCE d'ingestion (event Binance -> réception consumer) ---------
--    SLO cible : p95 < 15 s end-to-end. Ici on mesure event -> réception ;
--    ajouter ~5-10 s de commit Snowpipe Streaming pour l'end-to-end.
SELECT
    symbol,
    COUNT(*)                                                                AS trades_5min,
    ROUND(AVG(DATEDIFF('millisecond', traded_at, ingest_time))/1000.0, 3)   AS avg_latency_s,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
          ORDER BY DATEDIFF('millisecond', traded_at, ingest_time))/1000.0, 3) AS p95_latency_s
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
WHERE ingest_time >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
GROUP BY symbol
ORDER BY symbol;

-- 2) FRAÎCHEUR (dernier event / ingest vs maintenant) -----------------
--    SLO : warn > 30 s, error > 120 s.
SELECT
    'trades' AS source,
    MAX(traded_at)                                              AS last_event_time,
    MAX(ingest_time)                                            AS last_ingest_time,
    DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP())   AS freshness_seconds
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
UNION ALL
SELECT
    'depth',
    NULL,
    MAX(ingest_time),
    DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP())
FROM ANALYTICS.PUBLIC_STAGING.STG_DEPTH_LEVELS;

-- 3) DÉBIT (lignes/min, 15 dernières min) -----------------------------
SELECT
    DATE_TRUNC('minute', ingest_time) AS minute,
    COUNT(*)                          AS trades
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
WHERE ingest_time >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- 4) DYNAMIC TABLES — succès & lag des refreshs ------------------------
--    Historique des rafraîchissements (état SUCCEEDED attendu).
SELECT name, state, refresh_start_time, refresh_end_time, data_timestamp
FROM TABLE(ANALYTICS.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY refresh_end_time DESC
LIMIT 20;
--    Lag cible vs réel (target_lag / mean_lag / max_lag) :
SHOW DYNAMIC TABLES IN SCHEMA ANALYTICS.PUBLIC_MARTS;

-- 5) QUALITÉ — taux de déduplication (raw vs silver) ------------------
SELECT
    (SELECT COUNT(*) FROM RAW.CRYPTO.RAW_TRADES)                AS raw_trades,
    (SELECT COUNT(*) FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES)  AS dedup_trades,
    ROUND(100 * (1 - (SELECT COUNT(*) FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES)
                   / NULLIF((SELECT COUNT(*) FROM RAW.CRYPTO.RAW_TRADES), 0)), 1)
                                                                AS duplicate_pct;

-- 6) COÛT (crédits du warehouse, 24 dernières h) — FinOps --------------
SELECT
    DATE_TRUNC('hour', start_time) AS hour,
    ROUND(SUM(credits_used), 4)    AS credits
FROM TABLE(ANALYTICS.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
        DATE_RANGE_START => DATEADD('day', -1, CURRENT_DATE()),
        WAREHOUSE_NAME   => 'WH_CRYPTO_XS'))
GROUP BY 1
ORDER BY 1 DESC;
