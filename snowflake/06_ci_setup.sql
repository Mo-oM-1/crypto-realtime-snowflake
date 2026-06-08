-- =====================================================================
-- 06_ci_setup.sql - Environnement CI isole (validation des Pull Requests)
-- ---------------------------------------------------------------------
-- A executer UNE FOIS en ACCOUNTADMIN.
-- Principe : la CI build le projet dbt dans une base JETABLE (ANALYTICS_CI),
-- sous un role et un user de service DEDIES, en LECTURE SEULE sur la prod RAW.
-- => une PR ne peut jamais ecrire dans la prod ANALYTICS.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- 1) Base CI jetable (isolee de la prod ANALYTICS) --------------------
CREATE DATABASE IF NOT EXISTS ANALYTICS_CI
  COMMENT = 'Base jetable pour la CI (build + tests dbt des Pull Requests)';

-- 2) Role CI least-privilege ------------------------------------------
CREATE ROLE IF NOT EXISTS CRYPTO_CI_ROLE;
GRANT USAGE ON WAREHOUSE WH_CRYPTO_XS TO ROLE CRYPTO_CI_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CRYPTO_CI_ROLE;  -- parite (si modeles Cortex)

--    La CI POSSEDE sa base et son schema cible -> dbt y cree librement
--    schemas (PUBLIC_STAGING/...), tables, et l'objet DBT PROJECT.
GRANT OWNERSHIP ON DATABASE ANALYTICS_CI        TO ROLE CRYPTO_CI_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA   ANALYTICS_CI.PUBLIC TO ROLE CRYPTO_CI_ROLE COPY CURRENT GRANTS;

-- 3) Lecture SEULE sur les sources RAW (les modeles lisent RAW.CRYPTO.*)
GRANT USAGE  ON DATABASE RAW                       TO ROLE CRYPTO_CI_ROLE;
GRANT USAGE  ON SCHEMA   RAW.CRYPTO                 TO ROLE CRYPTO_CI_ROLE;
GRANT SELECT ON ALL    TABLES IN SCHEMA RAW.CRYPTO  TO ROLE CRYPTO_CI_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW.CRYPTO  TO ROLE CRYPTO_CI_ROLE;

-- 4) User de SERVICE dedie a la CI (cle RSA uniquement, distinct de SVC_CRYPTO)
CREATE USER IF NOT EXISTS SVC_CRYPTO_CI
  TYPE = SERVICE
  DEFAULT_ROLE = CRYPTO_CI_ROLE
  DEFAULT_WAREHOUSE = WH_CRYPTO_XS
  COMMENT = 'Service account - CI GitHub Actions (snow dbt deploy/execute)';
GRANT ROLE CRYPTO_CI_ROLE TO USER SVC_CRYPTO_CI;

-- 5) Authentification key-pair de la CI -------------------------------
--    Genere une paire DEDIEE (ne reutilise pas la cle de l'ingestion) :
--      openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ci_rsa_key.p8 -nocrypt
--      openssl rsa -in ci_rsa_key.p8 -pubout -out ci_rsa_key.pub
--    - cle PRIVEE  (contenu de ci_rsa_key.p8)  -> secret GitHub SNOWFLAKE_PRIVATE_KEY_RAW
--    - cle PUBLIQUE (contenu de ci_rsa_key.pub) -> a coller ci-dessous (sans en-tetes BEGIN/END)
-- ALTER USER SVC_CRYPTO_CI SET RSA_PUBLIC_KEY='<COLLE_TA_CLE_PUBLIQUE_CI_ICI>';

-- 6) (Optionnel) garde-fou cout DEDIE a la CI -------------------------
--    Evite qu'une rafale de PR n'epuise le quota de la prod (RM_CRYPTO).
-- CREATE RESOURCE MONITOR IF NOT EXISTS RM_CRYPTO_CI
--   WITH CREDIT_QUOTA = 1 FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY
--   TRIGGERS ON 100 PERCENT DO SUSPEND;

-- Verifs :
-- SHOW GRANTS TO ROLE CRYPTO_CI_ROLE;
-- SHOW USERS LIKE 'SVC_CRYPTO_CI';
