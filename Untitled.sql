-- =====================================================================
-- 04_drift_detection.sql — Détection AUTOMATISÉE de dérive de schéma
-- ---------------------------------------------------------------------
-- SQL pur, planifié (sans agent). Détecte les clés JSON nouvelles dans le
-- VARIANT brut et les logge. La REMÉDIATION reste agentique + revue humaine :
--   détection auto (ici)  ->  alerte  ->  `$check-schema-drift` (Cortex Code)
-- À exécuter une fois pour mettre en place la Task.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- 1) Référence des clés CONNUES (baseline) ----------------------------
--    Maintenue dans le temps : après chaque remédiation agentique, on y
--    ajoute la nouvelle clé pour qu'elle ne soit plus signalée (voir §5).
CREATE TABLE IF NOT EXISTS ANALYTICS.MONITORING.known_keys (
    source_table STRING,
    key_name     STRING
);

-- seed idempotent : ne s'insère QUE si la table est vide (préserve les ajouts ultérieurs)
INSERT INTO ANALYTICS.MONITORING.known_keys (source_table, key_name)
SELECT column1, column2
FROM VALUES
    ('RAW_TRADES','e'), ('RAW_TRADES','E'), ('RAW_TRADES','s'), ('RAW_TRADES','t'),
    ('RAW_TRADES','p'), ('RAW_TRADES','q'), ('RAW_TRADES','T'), ('RAW_TRADES','m'),
    ('RAW_TRADES','M'),
    ('RAW_DEPTH','lastUpdateId'), ('RAW_DEPTH','bids'), ('RAW_DEPTH','asks')
WHERE (SELECT COUNT(*) FROM ANALYTICS.MONITORING.known_keys) = 0;

-- 2) Vue de détection : clés vues sur 24h, absentes de la référence ---
CREATE OR REPLACE VIEW ANALYTICS.MONITORING.vw_schema_drift AS
SELECT 'RAW_TRADES' AS source_table, f.key AS new_key,
       TYPEOF(f.value) AS value_type, COUNT(*) AS occurrences
FROM RAW.CRYPTO.RAW_TRADES, LATERAL FLATTEN(input => RECORD:data) f
WHERE ingest_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND f.key NOT IN (SELECT key_name FROM ANALYTICS.MONITORING.known_keys
                    WHERE source_table = 'RAW_TRADES')
GROUP BY f.key, TYPEOF(f.value)
UNION ALL
SELECT 'RAW_DEPTH', f.key, TYPEOF(f.value), COUNT(*)
FROM RAW.CRYPTO.RAW_DEPTH, LATERAL FLATTEN(input => RECORD:data) f
WHERE ingest_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND f.key NOT IN (SELECT key_name FROM ANALYTICS.MONITORING.known_keys
                    WHERE source_table = 'RAW_DEPTH')
GROUP BY f.key, TYPEOF(f.value);

-- 3) TASK quotidienne : logge la dérive détectée ----------------------
CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_schema_drift_check
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'   -- chaque jour à 06:00 UTC (coût minime : 1 run/j)
  AS
  INSERT INTO ANALYTICS.MONITORING.pipeline_log (metric, value, status)
  SELECT 'schema_drift:' || source_table || ':' || new_key || ' (' || value_type || ')',
         occurrences,
         'DRIFT'
  FROM ANALYTICS.MONITORING.vw_schema_drift;

ALTER TASK ANALYTICS.MONITORING.crypto_schema_drift_check RESUME;

-- (Optionnel) Alerte e-mail si dérive — décommenter + e-mail vérifié
-- CREATE OR REPLACE ALERT ANALYTICS.MONITORING.crypto_schema_drift_alert
--   WAREHOUSE = WH_CRYPTO_XS
--   SCHEDULE  = 'USING CRON 5 6 * * * UTC'
--   IF (EXISTS (SELECT 1 FROM ANALYTICS.MONITORING.vw_schema_drift))
--   THEN CALL SYSTEM$SEND_EMAIL('crypto_email_int', 'ton.email@exemple.com',
--        'Dérive de schéma détectée',
--        'Nouvelles clés dans le VARIANT brut. Lance $check-schema-drift dans Cortex Code.');
-- ALTER ALERT ANALYTICS.MONITORING.crypto_schema_drift_alert RESUME;

-- 4) Vérifs -----------------------------------------------------------
-- SELECT * FROM ANALYTICS.MONITORING.vw_schema_drift;                    -- dérive courante
-- SELECT * FROM ANALYTICS.MONITORING.pipeline_log
--   WHERE status='DRIFT' ORDER BY checked_at DESC;                       -- historique
-- EXECUTE TASK ANALYTICS.MONITORING.crypto_schema_drift_check;           -- run manuel

-- 5) Boucle de gouvernance (après remédiation agentique) --------------
--    Quand `$check-schema-drift` a intégré une vraie nouvelle clé au staging,
--    ajoute-la à la référence pour clore la boucle :
-- INSERT INTO ANALYTICS.MONITORING.known_keys VALUES ('RAW_TRADES','<nouvelle_cle>');
