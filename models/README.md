# models/ — généré par Cortex Code

⚠️ **Ne pas écrire ces modèles à la main.** Ils sont produits par l'agent
**Cortex Code** (skill `$flatten-variant`) à partir des tables VARIANT
`RAW.CRYPTO.RAW_TRADES` et `RAW.CRYPTO.RAW_DEPTH`.

Structure attendue après exécution du skill :

```
models/
├── staging/        # vues — flatten des VARIANT (stg_trades, stg_depth_levels) + _sources.yml + _schema.yml
├── intermediate/   # tables — jointures / enrichissements (int_*)
└── marts/          # dimensions, faits, vues live + Dynamic Tables (dim_*, fct_*, vw_*_live)
```

Voir `runbook/cortex_code_prompts.md` pour les prompts exacts à lancer dans
Cortex Code (Snowsight) : `$flatten-variant` puis `$realtime-marts`.
