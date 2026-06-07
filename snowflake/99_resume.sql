-- =====================================================================
-- 99_resume.sql — Réveil du pipeline (avant une démo)
-- Inverse de 99_pause.sql. Pense à relancer le consumer ensuite.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;

-- Historique (Dynamic Tables)
ALTER DYNAMIC TABLE IF EXISTS ANALYTICS.PUBLIC_MARTS.FCT_OHLCV_1MIN          RESUME;
ALTER DYNAMIC TABLE IF EXISTS ANALYTICS.PUBLIC_MARTS.FCT_ORDERBOOK_SNAPSHOTS RESUME;

-- Monitoring (Alert + Tasks)
ALTER ALERT IF EXISTS ANALYTICS.MONITORING.CRYPTO_FRESHNESS_ALERT    RESUME;
ALTER TASK  IF EXISTS ANALYTICS.MONITORING.CRYPTO_DBT_TEST           RESUME;
ALTER TASK  IF EXISTS ANALYTICS.MONITORING.CRYPTO_SCHEMA_DRIFT_CHECK RESUME;
ALTER TASK  IF EXISTS ANALYTICS.MONITORING.CRYPTO_QUALITY_CHECK      RESUME;

-- Vérification
SHOW TASKS          IN SCHEMA ANALYTICS.MONITORING;
SHOW ALERTS         IN SCHEMA ANALYTICS.MONITORING;
SHOW DYNAMIC TABLES IN SCHEMA ANALYTICS.PUBLIC_MARTS;

-- Puis, sur ton Mac, relance l'ingestion :
--   cd ~/Documents/snowflake/ingestion && source venv/bin/activate && python stream_to_snowflake.py
