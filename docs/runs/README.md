# docs/runs — Runs documentés (preuves, pas captures)

Boucles agentiques **rejouables et auditables**. Chaque run documente une chaîne complète
*événement → action de l'agent → revue humaine → validation CI*, avec les vrais noms d'objets
et les artefacts (diff, commit, sortie SQL) — pas des screenshots.

| Run | Ce qu'il prouve | Artefacts |
|---|---|---|
| [`schema-drift-selfheal.md`](./schema-drift-selfheal.md) | Détection auto de dérive → remédiation additive par `$check-schema-drift` → revue → CI | `fixtures/inject_schema_drift.sql`, `fixtures/cleanup_schema_drift.sql` |
| [`caught-bug.md`](./caught-bug.md) | Un test généré par l'agent a attrapé un vrai bug (carnet croisé `best_bid > best_ask`) | commit `40a5970`, `tests/assert_fct_orderbook_snapshots_invariants.sql` |
| [`incident-channel-recovery.md`](./incident-channel-recovery.md) | Incident prod : canal Snowpipe invalide → RAW figé + disque saturé → 3 correctifs testés | `/healthz`, logs, `ingestion/stream_to_snowflake.py` |

> Principe de gouvernance commun : **l'agent propose, la CI prouve, l'humain merge.**
