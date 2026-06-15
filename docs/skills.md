# Les 3 skills agentiques (capabilities, pas des prompts)

Ce projet n'utilise pas l'agent en « vibe coding ». Chaque skill Cortex Code est une
**capability conçue et bornée** : une entrée précise qu'elle inspecte, une sortie déterministe,
et des **garde-fous** explicites. Les responsabilités ne se chevauchent pas (un seul propriétaire
par préoccupation). Définitions : [`.cortex/skills/*/SKILL.md`](../.cortex/skills/).

> Principe transverse : **l'agent propose, la CI prouve (`ANALYTICS_CI`), l'humain merge.**
> Boucles rejouables et tracées : [`docs/runs/`](./runs/).

> **Skill ≠ prompt.** Les **3 skills** ci-dessous sont des capabilities productisées
> (`.cortex/skills/*/SKILL.md`). À ne pas confondre avec `$realtime-marts`, un simple **prompt**
> réutilisable dans [`AGENTS.md`](../AGENTS.md) (recette pour itérer la couche marts).

---

## 1. `flatten-variant` — *Build*

| | |
|---|---|
| **Rôle** | Transformer le VARIANT brut en projet dbt 3 couches (staging → intermediate → marts). |
| **Déclenchement** | À la demande (construction / régénération de la couche de modélisation). |
| **Entrée inspectée** | Colonnes `VARIANT` du schéma, structure JSON (`LATERAL FLATTEN` + `TYPEOF`), `RECORD:stream` / `RECORD:data`. |
| **Sortie produite** | Modèles `stg_*` (vues, flatten + dedup), `int_*` (vues), `dim/fct_*` ; **tests structurels** (`not_null`/`unique` sur clés, `accepted_values`) + **doc des colonnes**. |
| **Garde-fous** | Conventions de cast (`NUMBER(38,8)`, epoch-ms → `TO_TIMESTAMP_NTZ(...,3)`, booléens, alias UPPER_CASE) ; staging en **vues** (zéro stockage, inspectable). |
| **Frontière** | **Possède** la structure + les tests de clés + la doc. Ne fait **pas** les règles métier (→ `generate-quality-tests`). |

## 2. `check-schema-drift` — *Maintain / self-heal*

| | |
|---|---|
| **Rôle** | Détecter les clés JSON nouvelles / changées non mappées, et **étendre le staging additivement** pour les absorber. |
| **Déclenchement** | Sur signal de dérive (`pipeline_log` status `DRIFT`, alimenté par la task `crypto_schema_drift_check`). |
| **Entrée inspectée** | Clés réelles de `RECORD:data` (RAW_TRADES / RAW_DEPTH) vs **mapping connu** (baseline) + colonnes actuelles du staging. |
| **Sortie produite** | Diff **additif** sur `stg_*` (nouvelle colonne castée par convention) + rapport de dérive (NEW / TYPE CHANGED / MISSING). |
| **Garde-fous** | **Additif & non destructif** (jamais de suppression auto → reporté pour décision humaine) ; **idempotent** (zéro dérive = zéro changement) ; conventions respectées ; **revue humaine du diff avant merge**. |
| **Frontière** | **Possède** le self-heal du flatten. Preuve rejouable : [`docs/runs/schema-drift-selfheal.md`](./runs/schema-drift-selfheal.md). |

## 3. `generate-quality-tests` — *Quality*

| | |
|---|---|
| **Rôle** | Profiler les modèles et générer les **tests métier** (ranges, invariants croisés) + **unit tests** de la logique de transfo. |
| **Déclenchement** | À la demande / sur échec de qualité. |
| **Entrée inspectée** | Profils des modèles (min/max/null/domaines), colonnes réelles par couche. |
| **Sortie produite** | Tests singuliers `tests/assert_<model>_<rule>.sql` (renvoient les lignes **violantes**) ; unit tests dbt 1.8 (RSI Wilder, OHLCV) ; **severity tiering** (`error` invariants durs / `warn` signaux) ; `store_failures: true`. |
| **Garde-fous** | Pas de `dbt_utils`/`dbt_expectations` (built-ins only) ; **n'ajoute pas** les tests structurels (déjà produits par `flatten-variant`) → zéro doublon. |
| **Frontière** | **Possède** toute la qualité métier. Preuve concrète : a attrapé un vrai bug → [`docs/runs/caught-bug.md`](./runs/caught-bug.md). |

---

## Pourquoi c'est de la *gouvernance*, pas de l'autonomie

- La **détection** est automatisée (SQL planifié), pas l'agent qui « cron ».
- La **remédiation** est proposée par l'agent mais **revue par un humain** avant tout commit.
- La **validation** passe par la CI sur une base isolée (`ANALYTICS_CI`, lecture seule sur RAW) :
  une PR ne peut jamais écrire en prod.
- Chaque capability est **bornée** (additive, idempotente, conventions) — pas de free-for-all.
