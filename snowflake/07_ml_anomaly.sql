-- =====================================================================
-- 07_ml_anomaly.sql - Couche de SURVEILLANCE : detection d'anomalies (Cortex ML)
-- ---------------------------------------------------------------------
-- But : donner un USAGE au pipeline. Au lieu d'afficher des chiffres bruts,
-- un modele ML APPREND le comportement normal du volume par symbole et heure,
-- puis flague ce qui en sort (avec intervalle de confiance). Ces anomalies
-- alimentent : (1) une alerte, (2) le panneau "surveillance" du dashboard.
--
-- Difference avec le z-score de VW_MARKET_METRICS_LIVE :
--   - z-score live = heuristique naive (seuil fixe, pas de saisonnalite) ;
--   - ce modele    = entraine, apprend la tendance/saisonnalite, borne de confiance.
--
-- Pre-requis : FCT_OHLCV_1MIN doit contenir assez d'historique (ideal : quelques
-- jours ; minimum : quelques heures de consumer en continu). Avec peu d'historique
-- le modele tourne quand meme mais flague moins finement.
-- A executer une fois (1-2 en ACCOUNTADMIN, le reste en CRYPTO_PIPELINE_ROLE).
-- =====================================================================

-- 1) Privilege ML (account-level) -------------------------------------
USE ROLE ACCOUNTADMIN;
-- Droit de creer un objet modele ML dans le schema MONITORING (comme CREATE TABLE).
GRANT CREATE SNOWFLAKE.ML.ANOMALY_DETECTION ON SCHEMA ANALYTICS.MONITORING TO ROLE CRYPTO_PIPELINE_ROLE;

-- 2) Contexte ---------------------------------------------------------
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;
-- (le schema ANALYTICS.MONITORING + la table pipeline_log sont crees par 03_alerts.sql)

-- 3) Vues de features (volume par symbole / minute) -------------------
--    SERIES = identifiant de serie (un modele multi-series, un sous-modele par symbole).
--    La doc recommande un VARIANT pour la colonne serie -> to_variant(symbol).
-- CONTRAINTE Snowflake AD : DETECT_ANOMALIES ne peut scorer que des timestamps
-- POSTERIEURS a la fin des donnees d'entrainement (le modele prevoit l'apres-T et
-- compare les nouveaux points a sa prevision). On coupe donc l'entrainement a now-2h,
-- et on score [now-2h, now]. Fenetres DISJOINTES et ORDONNEES.
CREATE OR REPLACE VIEW ANALYTICS.MONITORING.VW_VOLUME_HISTORY AS
  SELECT to_variant(symbol) AS symbol, minute, volume
  FROM ANALYTICS.PUBLIC_MARTS.FCT_OHLCV_1MIN
  WHERE minute >= dateadd('day', -7, sysdate())
    AND minute <  dateadd('hour', -2, sysdate());  -- ENTRAINEMENT : jusqu'a now-2h (exclu)

CREATE OR REPLACE VIEW ANALYTICS.MONITORING.VW_VOLUME_RECENT AS
  SELECT to_variant(symbol) AS symbol, minute, volume
  FROM ANALYTICS.PUBLIC_MARTS.FCT_OHLCV_1MIN
  WHERE minute >= dateadd('hour', -2, sysdate());  -- SCORING : [now-2h, now], apres l'entrainement

-- 4) Table de resultats (lue par l'alerte + le dashboard) -------------
CREATE TABLE IF NOT EXISTS ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES (
    scored_at   TIMESTAMP_NTZ,
    symbol      STRING,
    minute      TIMESTAMP_NTZ,
    volume      FLOAT,        -- Y : valeur observee
    forecast    FLOAT,        -- valeur attendue par le modele
    lower_bound FLOAT,        -- borne basse de l'intervalle de confiance
    upper_bound FLOAT,        -- borne haute
    is_anomaly  BOOLEAN,      -- TRUE si hors intervalle
    distance    FLOAT         -- z-score (distance a la prevision)
);

-- 5) Entrainement du modele (multi-series, NON supervise) -------------
--    LABEL_COLNAME = '' -> non supervise (on n'a pas d'anomalies etiquetees).
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANALYTICS.MONITORING.crypto_volume_ad(
    INPUT_DATA        => TABLE(ANALYTICS.MONITORING.VW_VOLUME_HISTORY),
    SERIES_COLNAME    => 'SYMBOL',
    TIMESTAMP_COLNAME => 'MINUTE',
    TARGET_COLNAME    => 'VOLUME',
    LABEL_COLNAME     => ''
);

-- 6) Procedure de scoring (idempotente sur la fenetre recente) --------
--    DELETE puis INSERT sur les 2 dernieres heures -> reecrit proprement,
--    pas de doublon de minute. prediction_interval 0.99 -> ~1% flague.
CREATE OR REPLACE PROCEDURE ANALYTICS.MONITORING.SP_SCORE_VOLUME_ANOMALIES()
  RETURNS STRING
  LANGUAGE SQL
AS
BEGIN
  DELETE FROM ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES
   WHERE minute >= dateadd('hour', -2, sysdate());

  INSERT INTO ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES
  SELECT
      sysdate()        AS scored_at,
      series::string   AS symbol,
      ts               AS minute,
      y                AS volume,
      forecast,
      lower_bound,
      upper_bound,
      is_anomaly,
      distance
  FROM TABLE(ANALYTICS.MONITORING.crypto_volume_ad!DETECT_ANOMALIES(
      INPUT_DATA        => TABLE(ANALYTICS.MONITORING.VW_VOLUME_RECENT),
      SERIES_COLNAME    => 'SYMBOL',
      TIMESTAMP_COLNAME => 'MINUTE',
      TARGET_COLNAME    => 'VOLUME',
      CONFIG_OBJECT     => {'prediction_interval': 0.99}
  ));

  RETURN 'scored';
END;

-- Premier scoring tout de suite (pour remplir la table / la demo)
CALL ANALYTICS.MONITORING.SP_SCORE_VOLUME_ANOMALIES();

-- 7) Orchestration ----------------------------------------------------
-- 7a) Re-entrainement quotidien (apprend le normal sur 7 jours glissants)
CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_anomaly_retrain
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '60 MINUTE'   -- horaire : la fin d'entrainement suit now-2h => horizon de prevision court
  AS
  CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANALYTICS.MONITORING.crypto_volume_ad(
      INPUT_DATA        => TABLE(ANALYTICS.MONITORING.VW_VOLUME_HISTORY),
      SERIES_COLNAME    => 'SYMBOL',
      TIMESTAMP_COLNAME => 'MINUTE',
      TARGET_COLNAME    => 'VOLUME',
      LABEL_COLNAME     => '');

-- 7b) Scoring regulier (toutes les 30 min) -> remplit MART_VOLUME_ANOMALIES
CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_anomaly_score
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '30 MINUTE'
  AS
  CALL ANALYTICS.MONITORING.SP_SCORE_VOLUME_ANOMALIES();

ALTER TASK ANALYTICS.MONITORING.crypto_anomaly_retrain RESUME;
ALTER TASK ANALYTICS.MONITORING.crypto_anomaly_score   RESUME;

-- 8) Alerte : prevenir quand une anomalie recente apparait ------------
CREATE OR REPLACE ALERT ANALYTICS.MONITORING.crypto_anomaly_alert
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = '15 MINUTE'
  IF (EXISTS (
        SELECT 1 FROM ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES
        WHERE is_anomaly AND minute >= dateadd('minute', -15, sysdate())
  ))
  THEN
  BEGIN
      -- 1) trace d'audit
      INSERT INTO ANALYTICS.MONITORING.pipeline_log (metric, value, status)
      SELECT 'volume_anomaly',
             COUNT(*),
             'ANOMALY'
      FROM ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES
      WHERE is_anomaly AND minute >= dateadd('minute', -15, sysdate());
      -- 2) notification reelle (email) - integration crypto_email_int (cf. 03_alerts.sql)
      CALL SYSTEM$SEND_EMAIL(
          'crypto_email_int',
          'r.tobias47@proton.me',
          'ALERTE crypto : anomalie de volume',
          'Le modele Cortex ML a flague un volume anormal sur les 15 dernieres minutes. '
          || 'Voir le dashboard (page Surveillance) ou MART_VOLUME_ANOMALIES.');
  END;

ALTER ALERT ANALYTICS.MONITORING.crypto_anomaly_alert RESUME;

-- 9) Verifs -----------------------------------------------------------
-- SELECT * FROM ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES WHERE is_anomaly ORDER BY minute DESC;
-- SELECT symbol, COUNT_IF(is_anomaly) AS anomalies, COUNT(*) AS pts
--   FROM ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES GROUP BY symbol;
-- SHOW TASKS  IN SCHEMA ANALYTICS.MONITORING;
-- CALL ANALYTICS.MONITORING.crypto_volume_ad!SHOW_EVALUATION_METRICS();   -- qualite du modele

-- Pour suspendre (eviter tout cout hors demo) :
-- ALTER TASK  ANALYTICS.MONITORING.crypto_anomaly_retrain SUSPEND;
-- ALTER TASK  ANALYTICS.MONITORING.crypto_anomaly_score   SUSPEND;
-- ALTER ALERT ANALYTICS.MONITORING.crypto_anomaly_alert   SUSPEND;
