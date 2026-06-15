# Étude de cas — un test généré par l'agent a attrapé un vrai bug

La meilleure preuve de valeur d'une couche de tests, ce n'est pas qu'elle soit verte :
c'est qu'elle **échoue quand il le faut**. Ici, un test d'invariant généré par
`$generate-quality-tests` a révélé un bug de **grain** dans le mart order book — un bug
silencieux qui produisait des **carnets croisés** (`best_bid > best_ask`, économiquement
impossible).

| | |
|---|---|
| **Détecté par** | `tests/assert_fct_orderbook_snapshots_invariants.sql` |
| **Modèle fautif** | `models/marts/fct_orderbook_snapshots.sql` |
| **Commit du fix** | `40a5970` — *fix fct_orderbook_snapshots (grain snapshot via last_update_id)* |
| **Capture** | `docs/screenshots/quality-test-bug-diagnosis.png`, `quality-test-bug-fix.png` |

## Le symptôme

Le test d'invariant échoue : des lignes avec `best_bid > best_ask`.

```sql
-- tests/assert_fct_orderbook_snapshots_invariants.sql
select *
from {{ ref('fct_orderbook_snapshots') }}
where best_bid > best_ask          -- carnet croisé : impossible
   or spread_bps < 0
   or imbalance < 0 or imbalance > 1
   or mid < best_bid - 1e-6 or mid > best_ask + 1e-6
```

Un carnet croisé n'existe pas sur un marché réel : c'est forcément un artefact de modélisation.

## La cause racine

Le mart agrégeait le carnet par `(symbol, ingest_time)`. Mais `ingest_time` est l'heure de
**réception** côté consumer : **plusieurs snapshots distincts du carnet peuvent partager le même
`ingest_time`** (arrivés dans la même seconde / le même micro-batch). En groupant par `ingest_time`,
le `max(best_bid)` d'un snapshot se retrouvait combiné au `min(best_ask)` d'un **autre** snapshot
→ un « carnet » croisé qui n'a jamais existé tel quel.

## Le fix (réel, commit `40a5970`)

Grain corrigé sur `last_update_id` — l'**identifiant de séquence du snapshot** fourni par Binance.
Une ligne = un vrai snapshot, donc bid/ask issus du **même** instant logique.

```diff
     select
         symbol,
-        ingest_time,
+        last_update_id,
+        max(ingest_time)                                             as ingest_time,
         max(case when side = 'bid' then price end)::number(38,8)      as best_bid,
         min(case when side = 'ask' then price end)::number(38,8)      as best_ask,
         ...
     from {{ ref('stg_depth_levels') }}
-    group by symbol, ingest_time
+    group by symbol, last_update_id
```

Le test d'invariant repasse **vert** : plus aucun carnet croisé.

## Pourquoi ça compte (gouvernance agentique)

- L'agent n'a pas seulement **généré** des tests — un de ces tests a **trouvé un défaut réel** que
  l'œil humain aurait laissé passer (le croisement était intermittent, dépendant du débit).
- La correction reste **humaine et tracée** (commit `40a5970`), pas une auto-réparation aveugle.
- C'est l'illustration concrète du principe du repo : **l'agent propose / outille, les tests
  prouvent, l'humain corrige et merge.**
