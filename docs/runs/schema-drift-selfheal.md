# Run documenté — Self-healing de dérive de schéma (rejouable)

Ce document **prouve** la boucle agentique gouvernée du projet : une dérive de schéma est
**détectée automatiquement** (SQL planifié), **remédiée par l'agent** (`$check-schema-drift`,
Cortex Code), **revue par un humain**, puis **validée par la CI** avant merge.

> Tout ici est **rejouable** : injecte la dérive → lance le skill → observe la réparation.
> Pas une capture d'écran, une séquence reproductible.

## La boucle (gouvernance, pas autonomie)

```
dérive dans RAW (VARIANT)
   │  détection AUTO (SQL planifié)
   ▼
crypto_schema_drift_check  ──►  pipeline_log (status='DRIFT')
   │  signal
   ▼
$check-schema-drift (agent)  ──►  diff additif proposé sur stg_trades
   │  REVUE HUMAINE (le diff, pas l'app)
   ▼
PR  ──►  CI build+test sur ANALYTICS_CI (base isolée)  ──►  merge si vert
   │  clôture
   ▼
known_keys += nouvelle clé   (la dérive ne sera plus signalée)
```

**Principe :** l'agent **propose**, la CI **prouve**, l'humain **merge**. Aucun commit aveugle.

## Prérequis

- Le pipeline est en place (`00_setup.sql`, `04_drift_detection.sql` exécutés).
- Cortex Code disponible (pour lancer `$check-schema-drift`).
- Idéalement : itérer sur la branche/`ANALYTICS_DEV` (cf. `09_dev_setup.sql`), pas en prod.

## Étape 1 — Injecter une dérive contrôlée

On ajoute un message `@trade` synthétique portant une **clé nouvelle** `xs` (un futur
champ « x_signal ») sous `RECORD:data`.

```sql
-- docs/runs/fixtures/inject_schema_drift.sql
```

Sentinelle : `trade id = 999000111` (pour un cleanup exact en fin de scénario).

## Étape 2 — Détection automatique

La vue de détection compare les clés vues dans `RECORD:data` à la baseline `known_keys` :

```sql
SELECT * FROM ANALYTICS.MONITORING.vw_schema_drift;
-- ou forcer le log via la task planifiée :
EXECUTE TASK ANALYTICS.MONITORING.crypto_schema_drift_check;
SELECT * FROM ANALYTICS.MONITORING.pipeline_log
WHERE status = 'DRIFT' ORDER BY checked_at DESC LIMIT 5;
```

**Sortie attendue** : une ligne `RAW_TRADES | xs | <type> | 1` dans `vw_schema_drift`,
et une entrée `schema_drift:RAW_TRADES:xs (...)` / `DRIFT` dans `pipeline_log`.

## Étape 3 — Remédiation agentique

Dans Cortex Code :

```
$check-schema-drift
```

Le skill profile le VARIANT, diffe contre le mapping connu, et trouve `xs` non couvert.
Conformément à ses garde-fous (**additif, non destructif, conventions respectées**), il
**propose** d'étendre `stg_trades` — sans casser les colonnes existantes :

```sql
-- ajout propose dans models/staging/stg_trades.sql (bloc flattened)
record:data:xs::number(38,8)   as x_signal,
```

## Étape 4 — Revue humaine + clôture de la boucle

1. **Relire le diff** (c'est ça, la gouvernance : on valide la proposition, pas l'app).
2. Appliquer sur une branche → ouvrir une **PR**.
   > ⚠️ `xs` est un champ **synthétique** (fixture) : la PR sert à **démontrer le gate CI**,
   > on ne la merge pas en prod. Avec une vraie nouvelle clé Binance, on mergerait.
3. Ajouter la clé à la baseline pour clore la boucle (elle ne sera plus signalée) :
   ```sql
   INSERT INTO ANALYTICS.MONITORING.known_keys VALUES ('RAW_TRADES', 'xs');
   ```

## Étape 5 — Validation (la CI prouve)

La PR déclenche la CI : `sqlfluff` + `dbt build` + tests sur **`ANALYTICS_CI`** (base isolée,
lecture seule sur RAW). Un échec **bloque** la PR. Vert → merge.

```sql
-- equivalent local
EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='build';
```

## Étape 6 — Cleanup

```sql
-- docs/runs/fixtures/cleanup_schema_drift.sql
```

---

## Capture du run réel

> Remplir après exécution — c'est ce qui transforme ce runbook en **preuve**.

- **Date / environnement** : _(ex. 2026-06-15, ANALYTICS_DEV)_
- **Ligne `pipeline_log` (DRIFT)** :
  ```
  (colle ici la ligne schema_drift:RAW_TRADES:xs ...)
  ```
- **Diff proposé par l'agent** (`git diff` sur `stg_trades.sql`) :
  ```diff
  (colle ici le diff)
  ```
- **Note de revue humaine** : _(ce que tu as vérifié sur le diff)_
- **PR de démonstration** : `#(numéro)` — CI verte (gate prouvé), non mergée (`xs` synthétique)
- **Build final** : _(vert / nb de tests passés)_
