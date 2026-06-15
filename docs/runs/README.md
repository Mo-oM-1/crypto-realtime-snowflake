# docs/runs — Runs documentés (preuves, pas captures)

Boucles agentiques **rejouables et auditables**. Chaque run documente une chaîne complète
*événement → action de l'agent → revue humaine → validation CI*, avec les vrais noms d'objets
et les artefacts (diff, commit, sortie SQL) — pas des screenshots.

| Run | Ce qu'il prouve | Fixtures |
|---|---|---|
| [`schema-drift-selfheal.md`](./schema-drift-selfheal.md) | Détection auto de dérive → remédiation additive par `$check-schema-drift` → revue → CI | `fixtures/inject_schema_drift.sql`, `fixtures/cleanup_schema_drift.sql` |

> Principe de gouvernance commun : **l'agent propose, la CI prouve, l'humain merge.**
