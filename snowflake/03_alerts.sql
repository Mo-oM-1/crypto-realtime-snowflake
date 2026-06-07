-- =====================================================================
-- 03_alerts.sql — Monitoring automatisé (prod) : Alert + Task
-- ---------------------------------------------------------------------
-- Met en place une surveillance continue : alerte de fraîcheur des données
-- et tests dbt planifiés (qualité). À exécuter une fois.
-- =====================================================================

-- 1) Privilèges (account-level) ---------------------------------------
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT EXECUTE TASK  ON ACCOUNT TO ROLE CRYPTO_PIPELINE_ROLE;

-- (Optionnel) Notification email — destinataires = users Snowflake vérifiés
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS crypto_email_int
  TYPE = EMAIL
  ENABLED = TRUE;
GRANT USAGE ON INTEGRATION crypto_email_int TO ROLE CRYPTO_PIPELINE_ROLE;

-- 2) Schéma + table de log de monitoring ------------------------------
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

CREATE SCHEMA IF NOT EXISTS ANALYTICS.MONITORING;

CREATE TABLE IF NOT EXISTS ANALYTICS.MONITORING.pipeline_log (
    checked_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric     STRING,
    value      NUMBER,
    status     STRING
);

-- 3) ALERT de fraîcheur ----------------------------------------------
--    Se déclenche si aucun trade ingéré depuis > 120 s (SLO error).
--    Schedule à 5 min pour limiter le coût (le warehouse se réveille à
--    chaque check). Pour une détection plus fine, passe à '1 MINUTE'
--    ou utilise une alerte serverless (sans WAREHOUSE).
CREATE OR REPLACE ALERT ANALYTICS.MONITORING.crypto_freshness_alert
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '5 MINUTE'
  IF (EXISTS (
        SELECT 1
        FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
        HAVING DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP()) > 120
  ))
  THEN
      INSERT INTO ANALYTICS.MONITORING.pipeline_log (metric, value, status)
      SELECT 'freshness_seconds',
             DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP()),
             'STALE'
      FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES;

ALTER ALERT ANALYTICS.MONITORING.crypto_freshness_alert RESUME;

--    Variante e-mail (décommenter ; remplace par ton e-mail vérifié) :
-- CREATE OR REPLACE ALERT ANALYTICS.MONITORING.crypto_freshness_email
--   WAREHOUSE = WH_CRYPTO_XS
--   SCHEDULE  = '5 MINUTE'
--   IF (EXISTS (
--         SELECT 1 FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
--         HAVING DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP()) > 120))
--   THEN CALL SYSTEM$SEND_EMAIL(
--         'crypto_email_int',
--         'ton.email@exemple.com',
--         'ALERTE pipeline crypto : données obsolètes',
--         'Aucun trade ingéré depuis > 120s. Vérifie le consumer.');
-- ALTER ALERT ANALYTICS.MONITORING.crypto_freshness_email RESUME;

-- 4) TASK — tests dbt planifiés (qualité de données) -----------------
--    Lance `dbt test` toutes les heures via l'objet dbt project natif.
CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_dbt_test
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '60 MINUTE'
  AS
  EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='test';

ALTER TASK ANALYTICS.MONITORING.crypto_dbt_test RESUME;

-- 5) Vérifs -----------------------------------------------------------
-- SHOW ALERTS IN SCHEMA ANALYTICS.MONITORING;
-- SHOW TASKS  IN SCHEMA ANALYTICS.MONITORING;
-- SELECT * FROM ANALYTICS.MONITORING.pipeline_log ORDER BY checked_at DESC;

-- Pour suspendre (éviter tout coût hors démo) :
-- ALTER ALERT ANALYTICS.MONITORING.crypto_freshness_alert SUSPEND;
-- ALTER TASK  ANALYTICS.MONITORING.crypto_dbt_test         SUSPEND;
