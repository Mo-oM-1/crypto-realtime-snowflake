# Runbook — Génération de la couche dbt par Cortex Code

Ce projet **n'écrit pas les modèles dbt à la main** : c'est l'agent **Cortex Code**
qui les génère à partir des tables VARIANT brutes (fidèle à la démo de Ferhat).

## Pré-requis

1. `snowflake/00_setup.sql` exécuté (bases, rôle, warehouse, tables, key-pair, resource monitor).
2. Le consumer tourne et alimente `RAW.CRYPTO.RAW_TRADES` / `RAW_DEPTH` (cf. README).
   Vérifie qu'il y a des données :
   ```sql
   SELECT 'trades' src, COUNT(*) n FROM RAW.CRYPTO.RAW_TRADES
   UNION ALL SELECT 'depth', COUNT(*) FROM RAW.CRYPTO.RAW_DEPTH;
   ```
3. **Cortex Code activé** et le repo ouvert (Snowsight → Cortex Code, ou CLI `cortex`),
   rôle actif `CRYPTO_PIPELINE_ROLE`. Le skill est lu depuis `.cortex/skills/flatten-variant/SKILL.md`,
   les prompts depuis `AGENTS.md`.

## Étape 1 — Flatten + projet dbt

Dans Cortex Code, tape :

```
$flatten-variant
```

(ou colle le prompt complet depuis `AGENTS.md`). L'agent va :

1. `DESCRIBE` / interroger `INFORMATION_SCHEMA` pour repérer la colonne `RECORD` (VARIANT).
2. Échantillonner la structure JSON (`OBJECT_KEYS`, `LATERAL FLATTEN`, `TYPEOF`).
3. Générer `models/staging/stg_trades.sql` et `stg_depth_levels.sql` (+ `_sources.yml`, `_schema.yml`).
4. Générer `models/intermediate/` puis `models/marts/`.
5. Lancer `dbt compile` → `dbt run` → `dbt test`.

➡️ **Revoir le diff Git** avant de commiter (gouvernance).

## Étape 2 — Marts temps réel (live + historique)

```
$realtime-marts
```

Génère les **vues live** (`vw_ohlcv_1min_live`, `vw_orderbook_metrics_live`,
`vw_market_metrics_live`) et les **Dynamic Tables** historiques
(`fct_ohlcv_1min`, `fct_orderbook_snapshots`) + `dim_symbols` (seed).

> Les vues live calculent à la lecture → latence = ingestion (~5-10 s).
> Les Dynamic Tables (target_lag 1 min) servent l'historique, hors chemin temps réel.

## Étape 3 — Charger le seed & exécuter

```bash
dbt seed     # charge dim_symbols
dbt build    # run + test de tout le projet
```

Avec **dbt Projects on Snowflake**, ces commandes s'exécutent nativement dans Snowflake
(et peuvent être planifiées via une **Snowflake Task** pour le redéploiement/les tests).
La fraîcheur temps réel, elle, vient de Snowpipe Streaming + des vues live — pas d'un run planifié.

## Étape 4 — Vérifier la fraîcheur / latence (SLO)

```sql
-- latence d'ingestion (event time -> chargement), trades
SELECT symbol,
       AVG(DATEDIFF('millisecond', traded_at, ingest_time))/1000.0 AS avg_latency_s
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES
WHERE ingest_time > DATEADD('minute', -5, CURRENT_TIMESTAMP())
GROUP BY symbol;

-- état des Dynamic Tables
SELECT name, state, target_lag_sec, mean_lag_sec
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY data_timestamp DESC LIMIT 20;
```

## Personnalisation

Pour changer les symboles ou ajouter une table, adapte le prompt `$flatten-variant`
dans `AGENTS.md` (remplace les noms de base/schéma/tables), puis relance.
