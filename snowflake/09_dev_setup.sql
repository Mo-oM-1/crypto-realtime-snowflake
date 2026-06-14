-- =====================================================================
-- 09_dev_setup.sql - Environnement DEV (iteration interactive, isole de la prod)
-- ---------------------------------------------------------------------
-- A executer UNE FOIS en ACCOUNTADMIN.
-- Principe : iterer sur les modeles dbt dans une base DEDIEE (ANALYTICS_DEV),
-- sans jamais toucher la prod ANALYTICS. On lit les MEMES sources RAW (donnees
-- reelles du consumer) -> on teste la logique de transfo sur du vrai trafic.
--
-- Pyramide d'environnements (3 niveaux, bases isolees) :
--   dev  -> ANALYTICS_DEV  (interactif, Workspace)   - ce script
--   ci   -> ANALYTICS_CI   (validation des PR)        - 06_ci_setup.sql
--   prod -> ANALYTICS      (deploiement de prod)      - 00_setup.sql
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- 1) Base de dev isolee de la prod ------------------------------------
CREATE DATABASE IF NOT EXISTS ANALYTICS_DEV
  COMMENT = 'Base de developpement (iteration dbt interactive, isolee de la prod ANALYTICS)';

-- 2) Le role pipeline POSSEDE la base dev -> dbt y cree librement
--    schemas (PUBLIC_STAGING / PUBLIC_INTERMEDIATE / PUBLIC_MARTS), tables, DBT PROJECT.
GRANT OWNERSHIP ON DATABASE ANALYTICS_DEV        TO ROLE CRYPTO_PIPELINE_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA   ANALYTICS_DEV.PUBLIC TO ROLE CRYPTO_PIPELINE_ROLE COPY CURRENT GRANTS;

-- 3) Sources RAW : CRYPTO_PIPELINE_ROLE lit deja RAW.CRYPTO (role de prod),
--    donc les modeles dev lisent les memes donnees brutes -> rien a ajouter.

-- Verifs :
-- SHOW DATABASES LIKE 'ANALYTICS_DEV';
-- SHOW GRANTS ON DATABASE ANALYTICS_DEV;
