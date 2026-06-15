# models/ - généré par Cortex Code

Ces modèles ont été **générés par l'agent Cortex Code** — le **skill** `flatten-variant` (scaffold
staging → intermediate → marts) puis le **prompt** `$realtime-marts` (recette d'`AGENTS.md` pour itérer
la couche marts temps réel ; ce n'est pas un skill) — à partir des tables VARIANT
`RAW.CRYPTO.RAW_TRADES` / `RAW_DEPTH`, puis versionnés ici.

Structure :

- `staging/` : vues - flatten des VARIANT (`stg_trades`, `stg_depth_levels`) + `_sources.yml` + `_schema.yml`.
- `intermediate/` : vues - enrichissements (`int_trades_enriched`, `int_depth_levels`).
- `marts/` : dimension (`dim_symbols`, via le seed), faits incrémentaux (`fct_*`), vues live (`vw_*_live`) + 1 Dynamic Table (`fct_orderbook_snapshots`).

Pour régénérer après une évolution du schéma source, **relancer les skills** (voir
`runbook/cortex_code_prompts.md`) plutôt que d'éditer ces fichiers à la main.
