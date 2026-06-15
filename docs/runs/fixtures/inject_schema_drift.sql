-- =====================================================================
-- FIXTURE : injecte une derive de schema controlee pour rejouer le self-heal.
-- ---------------------------------------------------------------------
-- Insere UN message @trade synthetique dans RAW_TRADES avec une cle NOUVELLE
-- (`xs`, qui simule un nouveau champ "x_signal") sous RECORD:data.
-- Le detecteur (04_drift_detection.sql) la verra comme une cle hors `known_keys`.
--
-- Sentinelle : trade id 999000111 -> permet un cleanup EXACT (cf. cleanup_schema_drift.sql).
-- A lancer en CRYPTO_PIPELINE_ROLE. Scenario complet : docs/runs/schema-drift-selfheal.md
-- =====================================================================
USE ROLE CRYPTO_PIPELINE_ROLE;
USE WAREHOUSE WH_CRYPTO_XS;

INSERT INTO RAW.CRYPTO.RAW_TRADES (RECORD, INGEST_TIME)
SELECT PARSE_JSON($${
  "stream": "btcusdt@trade",
  "data": {
    "e": "trade",
    "E": 1718000000000,
    "s": "BTCUSDT",
    "t": 999000111,
    "p": "65000.00",
    "q": "0.001",
    "T": 1718000000000,
    "m": false,
    "M": true,
    "xs": 0.42
  }
}$$), CURRENT_TIMESTAMP();

-- Verif immediate : la cle `xs` doit apparaitre comme derive.
SELECT * FROM ANALYTICS.MONITORING.vw_schema_drift;
