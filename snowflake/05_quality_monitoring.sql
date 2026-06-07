-- =====================================================================
-- 05_quality_monitoring.sql — Surveillance des échecs de tests qualité
-- ---------------------------------------------------------------------
-- NOTE : la 1re version utilisait une ALERTE avec INFORMATION_SCHEMA.TASK_HISTORY
-- en condition — non supporté (une alerte n'accepte pas une fonction de table).
-- Version robuste : une TASK (qui, elle, exécute du SQL arbitraire) logge les
-- échecs de la task de tests dbt dans pipeline_log.
--   génération (agent) -> exécution horaire (crypto_dbt_test) -> log d'échec (ici) -> correction
-- Créée SUSPENDED (cohérent avec l'état en veille). RESUME avant une démo.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_quality_check
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = 'USING CRON 10 * * * * UTC'   -- chaque heure à HH:10 (après crypto_dbt_test)
  AS
  INSERT INTO ANALYTICS.MONITORING.pipeline_log (metric, value, status)
  SELECT 'dbt_test_failed:' || name, NULL, 'TEST_FAILED'
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'CRYPTO_DBT_TEST',
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
  WHERE state = 'FAILED';

-- Laissée SUSPENDED volontairement (coût). Avant une démo :
--   ALTER TASK ANALYTICS.MONITORING.crypto_quality_check RESUME;

-- Vérifs / exploitation -----------------------------------------------
-- Échecs de tests qualité loggés :
-- SELECT * FROM ANALYTICS.MONITORING.pipeline_log
--   WHERE status = 'TEST_FAILED' ORDER BY checked_at DESC;
-- Statut brut des runs de la task de tests :
-- SELECT name, state, scheduled_time, error_message
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'CRYPTO_DBT_TEST'))
--   ORDER BY scheduled_time DESC LIMIT 10;

-- (Optionnel) e-mail sur échec : ajouter SYSTEM$SEND_EMAIL dans le corps de la task
-- (intégration crypto_email_int) si tu veux une notification active.
