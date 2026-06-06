-- =====================================================================
-- 00_setup.sql — Setup Snowflake pour le pipeline crypto temps réel
-- ---------------------------------------------------------------------
-- À exécuter dans une RÉGION AWS (le SDK Python Snowpipe Streaming
-- haute-performance ET Cortex Code sont GA sur AWS).
-- Exécuter les étapes 1-5 et 8 en ACCOUNTADMIN ; 6-7 en CRYPTO_PIPELINE_ROLE.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- 1) Warehouse dédié, petit, auto-suspend agressif (FinOps) -----------
CREATE WAREHOUSE IF NOT EXISTS WH_CRYPTO_XS
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Pipeline crypto temps réel';

-- 2) Rôle dédié least-privilege ---------------------------------------
CREATE ROLE IF NOT EXISTS CRYPTO_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE WH_CRYPTO_XS TO ROLE CRYPTO_PIPELINE_ROLE;
-- Accès aux fonctions Cortex (briefing IA / forecasting plus tard, optionnel)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CRYPTO_PIPELINE_ROLE;

-- 3) Bases : RAW (atterrissage) + ANALYTICS (modèles dbt) -------------
CREATE DATABASE IF NOT EXISTS RAW;
CREATE SCHEMA   IF NOT EXISTS RAW.CRYPTO;
CREATE DATABASE IF NOT EXISTS ANALYTICS;

-- Ownership au rôle dédié (simplifie les droits pour Cortex Code & dbt,
-- qui créeront les schémas STAGING / INTERMEDIATE / MARTS dans ANALYTICS)
GRANT OWNERSHIP ON DATABASE RAW        TO ROLE CRYPTO_PIPELINE_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA   RAW.CRYPTO TO ROLE CRYPTO_PIPELINE_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON DATABASE ANALYTICS  TO ROLE CRYPTO_PIPELINE_ROLE COPY CURRENT GRANTS;

-- 4) Utilisateur de SERVICE dédié (pour l'ingestion) -----------------
--    Bonne pratique : un user TYPE = SERVICE (clé RSA uniquement, ni mot
--    de passe ni MFA), distinct de ton compte humain.
CREATE USER IF NOT EXISTS SVC_CRYPTO
  TYPE = SERVICE
  DEFAULT_ROLE = CRYPTO_PIPELINE_ROLE
  DEFAULT_WAREHOUSE = WH_CRYPTO_XS
  COMMENT = 'Service account — ingestion Snowpipe Streaming crypto';
GRANT ROLE CRYPTO_PIPELINE_ROLE TO USER SVC_CRYPTO;

--    Attribue AUSSI le rôle à ton utilisateur humain (pour dbt / Cortex Code) :
--    >>> remplace TOBIASR par ton login Snowflake <<<
GRANT ROLE CRYPTO_PIPELINE_ROLE TO USER TOBIASR;

-- 5) Authentification key-pair (Snowpipe Streaming) -------------------
--    Génère la clé en local :
--      openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--      openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--    Clé publique déjà enregistrée ci-dessous (générée le 2026-06-06).
--    C'est SVC_CRYPTO que le consumer utilise (via profile.json).
ALTER USER SVC_CRYPTO SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhSQ28YfhOfaP7xtDFCudaQK04ByStOSwks3GUpoibi0ToPsaSOY5NbCZ03WC/WvDk4ha8ENk0wSREVRG5Ad0hh8vztklOmaoXSczkS6yLXlahIXr5pUv3RFCycxd3YfTGgrbawCaL8yS08UdKglutwq9vd944dByEUC2COCI3N8ODsaOeEHbaX6TN90W3GzQHGZjUQLsoy4bseNHqKYm6d0YdJAwQ5WFbNyP0g45eo1T0H86DupQCyM39vlWM/w1dc6yB5jyVLT4pewTknWFuUpyxzZ3sXhMBPNUa7rFVefjrrkvot8eFxTy0PMAsXv1PcX0/WwsiYeeBlEF8w/AgwIDAQAB';

-- ---------------------------------------------------------------------
-- 6) Tables Bronze (VARIANT brut) — alimentées par Snowpipe Streaming
-- ---------------------------------------------------------------------
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

CREATE TABLE IF NOT EXISTS RAW.CRYPTO.RAW_TRADES (
  RECORD      VARIANT,                                   -- message Binance @trade brut (combined stream)
  INGEST_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- horodatage de chargement (mesure de latence)
);

CREATE TABLE IF NOT EXISTS RAW.CRYPTO.RAW_DEPTH (
  RECORD      VARIANT,                                   -- message Binance @depth brut (bids/asks = tableaux imbriqués)
  INGEST_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- NB : AUCUN "CREATE PIPE" requis. L'architecture haute-performance crée
-- un "default pipe" nommé <TABLE>-STREAMING à la 1ère ouverture de canal :
--   RAW_TRADES-STREAMING  et  RAW_DEPTH-STREAMING
-- (le consumer Python utilise ces noms automatiquement).

-- 7) Vérifs rapides ---------------------------------------------------
-- SELECT COUNT(*) FROM RAW.CRYPTO.RAW_TRADES;
-- SELECT RECORD, INGEST_TIME FROM RAW.CRYPTO.RAW_DEPTH LIMIT 2;

-- ---------------------------------------------------------------------
-- 8) Resource Monitor (garde-fou coût) — cap quotidien
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE RESOURCE MONITOR IF NOT EXISTS RM_CRYPTO
  WITH CREDIT_QUOTA = 1            -- 1 crédit / jour (ajuste selon ton budget)
  FREQUENCY = DAILY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 80  PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;
ALTER WAREHOUSE WH_CRYPTO_XS SET RESOURCE_MONITOR = RM_CRYPTO;
