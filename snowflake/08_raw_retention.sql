-- =====================================================================
-- 08_raw_retention.sql - Retention sur les tables Bronze (RAW)
-- ---------------------------------------------------------------------
-- PROBLEME : stg_trades fait un QUALIFY row_number() (dedup) sur TOUT RAW_TRADES.
-- Sans purge, RAW grossit sans fin -> le scan des vues live grossit avec, sans borne.
-- Rien ne purgeait RAW aujourd'hui.
--
-- SOLUTION : une task quotidienne purge les lignes plus vieilles que RETENTION.
-- RAW n'est qu'un BUFFER d'atterrissage : l'historique long terme vit deja dans
-- ANALYTICS (marts incrementaux fct_*, qui MERGENT et conservent leur propre etat).
-- Purger RAW ne perd donc PAS l'historique des marts.
--
-- CAVEAT : apres purge, un `dbt build --full-refresh` ne reconstruirait l'historique
-- que sur la fenetre de retention (RAW = buffer, pas source de verite long terme).
-- Pour un pipeline temps reel c'est le bon compromis ; ne full-refresh pas en esperant
-- retrouver des donnees plus vieilles que la retention.
-- A executer une fois.
-- =====================================================================

-- Pre-requis : privilege EXECUTE TASK (deja accorde dans 03_alerts.sql).
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- Retention = 7 jours (LARGE : couvre le lookback incremental (5 min), la fenetre des
-- vues live (120 min), et une marge de reprise/backfill). Ajuste selon ton budget/besoin.
-- Pattern Snowflake : la PROCEDURE porte la logique (reutilisable, appelable a la main),
-- la TASK porte le planning. (Meme separation que le scoring ML.)
CREATE OR REPLACE PROCEDURE ANALYTICS.MONITORING.SP_PURGE_RAW()
  RETURNS STRING
  LANGUAGE SQL
AS
BEGIN
  -- Retention 7 jours (RAW = buffer ; l'historique long terme vit dans les marts ANALYTICS).
  DELETE FROM RAW.CRYPTO.RAW_TRADES WHERE ingest_time < dateadd('day', -7, sysdate());
  DELETE FROM RAW.CRYPTO.RAW_DEPTH  WHERE ingest_time < dateadd('day', -7, sysdate());
  RETURN 'purge RAW > 7j ok';
END;

CREATE OR REPLACE TASK ANALYTICS.MONITORING.crypto_raw_retention
  WAREHOUSE = WH_CRYPTO_XS
  SCHEDULE  = 'USING CRON 0 4 * * * UTC'    -- chaque jour a 04:00 UTC (faible activite)
  AS
  CALL ANALYTICS.MONITORING.SP_PURGE_RAW();

ALTER TASK ANALYTICS.MONITORING.crypto_raw_retention RESUME;

-- (Optionnel) purge immediate au 1er setup :
-- CALL ANALYTICS.MONITORING.SP_PURGE_RAW();

-- ---------------------------------------------------------------------
-- VERIFICATION (a faire une fois, en conditions reelles)
-- ---------------------------------------------------------------------
-- 1) Taille / anciennete de RAW (doit rester bornee une fois la task active) :
-- SELECT 'raw_trades' AS t, COUNT(*) AS n, MIN(ingest_time) AS plus_vieux FROM RAW.CRYPTO.RAW_TRADES
-- UNION ALL SELECT 'raw_depth', COUNT(*), MIN(ingest_time) FROM RAW.CRYPTO.RAW_DEPTH;
--
-- 2) Pushdown du filtre temporel sous le QUALIFY : regarde le QUERY PROFILE d'une vue live
--    (Snowsight -> Query History -> Query Profile) et verifie le nombre de partitions scannees
--    vs total. Avec la retention, le scan est BORNE meme si le filtre ne descend pas sous la
--    window function. Si le profil montre encore un scan trop large APRES retention, envisage
--    un clustering de RAW par ingest_time (a peser : le clustering a un cout de maintenance).

-- Pour suspendre (hors demo) :
-- ALTER TASK ANALYTICS.MONITORING.crypto_raw_retention SUSPEND;
