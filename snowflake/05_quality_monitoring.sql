-- =====================================================================
-- 05_quality_monitoring.sql — Alerte sur échec des tests qualité
-- ---------------------------------------------------------------------
-- Les tests générés par `$generate-quality-tests` sont exécutés en continu
-- par la Task horaire `crypto_dbt_test` (03_alerts.sql). Ce script ajoute
-- une ALERTE qui te prévient quand un run de tests ÉCHOUE.
--   génération (agent) -> exécution horaire (task) -> alerte échec (ici) -> correction
-- À exécuter une fois.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- Alerte : si la Task de tests dbt a échoué dans la dernière heure -> log
CREATE OR REPLACE ALERT ANALYTICS.MONITORING.crypto_quality_alert
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '60 MINUTE'
  IF (EXISTS (
        SELECT 1
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
              TASK_NAME => 'CRYPTO_DBT_TEST',
              SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
        WHERE state = 'FAILED'
  ))
  THEN
      INSERT INTO ANALYTICS.MONITORING.pipeline_log (metric, value, status)
      SELECT 'dbt_test_failed:' || name, NULL, 'TEST_FAILED'
      FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
              TASK_NAME => 'CRYPTO_DBT_TEST',
              SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
      WHERE state = 'FAILED';

ALTER ALERT ANALYTICS.MONITORING.crypto_quality_alert RESUME;

-- (Optionnel) version e-mail — décommenter + e-mail vérifié
-- ... THEN CALL SYSTEM$SEND_EMAIL('crypto_email_int','ton.email@exemple.com',
--        'Tests qualité en échec', 'Un run de tests dbt a échoué. Vérifie le pipeline.');

-- Vérifs / exploitation -----------------------------------------------
-- SELECT * FROM ANALYTICS.MONITORING.pipeline_log
--   WHERE status='TEST_FAILED' ORDER BY checked_at DESC;
-- SELECT name, state, scheduled_time, error_message
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'CRYPTO_DBT_TEST'))
--   ORDER BY scheduled_time DESC LIMIT 10;

-- FinOps : cette alerte réveille le warehouse 1×/h. Hors démo, suspends-la :
-- ALTER ALERT ANALYTICS.MONITORING.crypto_quality_alert SUSPEND;
