# models/ - généré par Cortex Code

Ces modèles ont été **générés par l'agent Cortex Code** (`$flatten-variant` puis `$realtime-marts`)
à partir des tables VARIANT `RAW.CRYPTO.RAW_TRADES` / `RAW_DEPTH`, puis versionnés ici.

Structure :

- `staging/` : vues - flatten des VARIANT (`stg_trades`, `stg_depth_levels`) + `_sources.yml` + `_schema.yml`.
- `intermediate/` : tables - enrichissements (`int_trades_enriched`, `int_depth_levels`).
- `marts/` : dimensions, faits, vues live et Dynamic Tables (`dim_symbols`, `fct_*`, `vw_*_live`).

Pour régénérer après une évolution du schéma source, **relancer les skills** (voir
`runbook/cortex_code_prompts.md`) plutôt que d'éditer ces fichiers à la main.
