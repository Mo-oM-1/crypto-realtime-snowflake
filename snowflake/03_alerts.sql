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

-- Notification email. PREREQUIS : le destinataire doit etre un e-mail VERIFIE
-- d'un user Snowflake du compte (Snowsight -> profil -> Verify email), sinon
-- SYSTEM$SEND_EMAIL echoue. ALLOWED_RECIPIENTS borne les destinataires autorises.
CREATE OR REPLACE NOTIFICATION INTEGRATION crypto_email_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('r.tobias47@proton.me');
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
--    Schedule à 15 min : compromis FinOps (chaque check réveille le warehouse,
--    facturé min 60 s). Pour une détection plus fine, baisse à '5 MINUTE'
--    ou bascule sur une alerte serverless (sans WAREHOUSE).
CREATE OR REPLACE ALERT ANALYTICS.MONITORING.crypto_freshness_alert
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '15 MINUTE'
  IF (EXISTS (
        SELECT 1
        FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
        HAVING DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP()) > 120
  ))
  THEN
  BEGIN
      -- 1) trace d'audit (historique des incidents)
      INSERT INTO ANALYTICS.MONITORING.pipeline_log (metric, value, status)
      SELECT 'freshness_seconds',
             DATEDIFF('second', MAX(ingest_time), CURRENT_TIMESTAMP()),
             'STALE'
      FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES;
      -- 2) notification reelle (email)
      CALL SYSTEM$SEND_EMAIL(
          'crypto_email_int',
          'r.tobias47@proton.me',
          'ALERTE pipeline crypto : donnees obsoletes',
          'Aucun trade ingere depuis plus de 120 s. Verifie le consumer : '
          || 'sudo systemctl status crypto-ingest (VM) puis curl localhost:8000/healthz.');
  END;

ALTER ALERT ANALYTICS.MONITORING.crypto_freshness_alert RESUME;

-- NB : tant que la donnee reste obsolete, l'alerte renvoie un e-mail a chaque
-- evaluation (toutes les 15 min) - comportement "re-alerte" facon astreinte.
-- Pour n'alerter qu'au front (1re detection), il faudrait tracker l'etat precedent.

-- 4) TASK — build dbt planifié (rafraîchit l'incrémental + tests) ----
--    `dbt build` = run (rafraîchit les modèles INCREMENTAL : fct_ohlcv_1min, etc.) + tests.
--    (Le temps réel vient des vues live ; l'historique se rafraîchit ici, toutes les heures.)
CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_dbt_test
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '60 MINUTE'
  AS
  EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='build';

ALTER TASK ANALYTICS.MONITORING.crypto_dbt_test RESUME;

-- 5) Vérifs -----------------------------------------------------------
-- SHOW ALERTS IN SCHEMA ANALYTICS.MONITORING;
-- SHOW TASKS  IN SCHEMA ANALYTICS.MONITORING;
-- SELECT * FROM ANALYTICS.MONITORING.pipeline_log ORDER BY checked_at DESC;

-- Pour suspendre (éviter tout coût hors démo) :
-- ALTER ALERT ANALYTICS.MONITORING.crypto_freshness_alert SUSPEND;
-- ALTER TASK  ANALYTICS.MONITORING.crypto_dbt_test         SUSPEND;
