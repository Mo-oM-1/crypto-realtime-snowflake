-- =====================================================================
-- CLEANUP du scenario de derive de schema (inverse de inject_schema_drift.sql).
-- ---------------------------------------------------------------------
-- Retire la ligne sentinelle de RAW et, si tu veux REJOUER le scenario,
-- retire `xs` de la baseline known_keys (sinon il ne sera plus signale).
-- A lancer en CRYPTO_PIPELINE_ROLE.
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

-- 1) Supprime la ligne synthetique (sentinelle trade id 999000111)
DELETE FROM RAW.CRYPTO.RAW_TRADES
WHERE RECORD:data:t::number = 999000111;

-- 2) (Optionnel) pour rejouer le scenario depuis zero : retire xs de la baseline
-- DELETE FROM ANALYTICS.MONITORING.known_keys
-- WHERE source_table = 'RAW_TRADES' AND key_name = 'xs';

-- Verif : plus aucune derive residuelle attendue
SELECT * FROM ANALYTICS.MONITORING.vw_schema_drift;
