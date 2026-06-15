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

Exécuté le **2026-06-15** (compte `vb96941`, région eu-central-1). Chaîne complète vérifiée.

**1. Dérive détectée** (`vw_schema_drift`) :
```
RAW_TRADES    xs    DECIMAL    1
```

**2. Tracée dans `pipeline_log`** (status = DRIFT) :
```
2026-06-15 07:02:36.999    schema_drift:RAW_TRADES:xs (DECIMAL)    1    DRIFT
```

**3. Remédiation proposée par `$check-schema-drift`** — extension **additive** de
`models/staging/stg_trades.sql` (CTE `flattened` + select final), aucune colonne existante touchée :
```diff
     flattened as (
         select
             ...
             record:data:m::boolean        as is_buyer_market_maker,
+            record:data:xs::number(38,8)  as x_signal,
             ingest_time
         from source
     )
     select
         ...
         is_buyer_market_maker,
+        x_signal,
         ingest_time
     from flattened
```
Rapport textuel **réel** de l'agent (`$check-schema-drift`) pour ce run :

```
Drift report
RAW.CRYPTO.RAW_TRADES — drift found
- NEW key: xs (DECIMAL, 1 occurrence — matches the injected fixture sentinel trade 999000111).
  - Action: additively added `record:data:xs::number(38,8) as x_signal` to
    models/staging/stg_trades.sql (both the flatten CTE and final select). No existing columns touched.
- Top-level RECORD keys (stream, data) — as expected, no drift.
- All known keys (E,e,T,t,s,p,q,m,M) still present and type-stable.
RAW.CRYPTO.RAW_DEPTH — no drift.
- RECORD:data keys = lastUpdateId (INTEGER), bids (ARRAY), asks (ARRAY) — all covered. Staging in sync.
Validation: EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='build --select stg_trades'
  → PASS=10 WARN=0 ERROR=0.
Note: x_signal nullable (1 ligne sur ~2,9 M) → pas de test not_null. Additif, prêt pour revue humaine.
```

<details><summary>Capture (illustration, optionnelle)</summary>

![Rapport de dérive de l'agent](../screenshots/schema-drift-selfheal-report.png)

</details>

**4. Revue humaine** : diff conforme aux garde-fous — additif, cast `NUMBER(38,8)` (convention
montant/quantité), alias UPPER_CASE, dedup intact. Aucune suppression de colonne. ✓

**5. Validation** : `EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='build --select stg_trades'`
→ **PASS=10 WARN=0 ERROR=0** (tous les tests verts).

**6. Issue** : `xs` étant un champ **synthétique** (fixture), le changement n'est **pas mergé** en prod
— on a démontré la boucle, puis nettoyé (`cleanup_schema_drift.sql` + revert du staging). Avec une
vraie clé Binance, on aurait ouvert la PR et mergé après CI verte.
