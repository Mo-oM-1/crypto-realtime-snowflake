# Crypto Real-Time Analytics — Snowflake + dbt + Cortex Code

Pipeline crypto **100 % temps réel** (latence ~5–15 s), *production-grade*, 100 % écosystème
**Snowflake + dbt**, à **coût quasi nul**. La couche de transformation (flatten du JSON
imbriqué → modèles dbt) est **générée par l'agent Cortex Code** (skill `$flatten-variant`),
dans l'esprit de l'« agentic data engineering ».

- **Source** : Binance WebSocket — `@trade` (transactions) + `@depth` (carnet d'ordres, JSON imbriqué).
- **Ingestion** : **Snowpipe Streaming** (SDK Python) → tables Bronze en **VARIANT brut**.
- **Modélisation** : **Cortex Code** génère staging → intermediate → marts (dbt Projects on Snowflake).
- **Service** : vues **live** (calcul à la lecture) pour le temps réel ; **Dynamic Tables** pour l'historique ; dashboard **Streamlit**.

Plan détaillé : voir [`PROJECT_PLAN.md`](./PROJECT_PLAN.md).

## Architecture

```
Binance WS (@trade + @depth)
   │  Consumer Python — Snowpipe Streaming (open_channel / append_row, ~5-10s)
   ▼
RAW.CRYPTO.RAW_TRADES / RAW_DEPTH   (Bronze, VARIANT brut)
   │  ⟵  Cortex Code (agent, build-time) : détecte VARIANT → LATERAL FLATTEN → génère dbt
   ▼
staging (vues, flatten) → intermediate (tables) → marts
   ├─ LIVE : vw_ohlcv_1min_live, vw_orderbook_metrics_live, vw_market_metrics_live  (vues, temps réel)
   └─ HISTO : fct_ohlcv_1min, fct_orderbook_snapshots  (Dynamic Tables, lag 1 min)
   ▼
Streamlit (auto-refresh) · Observabilité / SLO
```

## Structure du repo

```
.
├── snowflake/00_setup.sql            # bases, rôle, warehouse, tables VARIANT, key-pair, resource monitor
├── ingestion/                        # consumer temps réel (NE flatten PAS — ingestion brute)
│   ├── stream_to_snowflake.py        #   Binance WS (2 flux) → Snowpipe Streaming → RAW VARIANT
│   ├── requirements.txt · Dockerfile · profile.json.example
├── .cortex/skills/flatten-variant/SKILL.md   # skill Cortex Code (flatten VARIANT → dbt)
├── AGENTS.md                         # prompts réutilisables : $flatten-variant, $realtime-marts
├── macros/                           # macros de flatten réutilisées par l'agent
├── dbt_project.yml · profiles.example.yml · packages.yml
├── seeds/dim_symbols.csv
├── models/                           # VIDE au départ — GÉNÉRÉ par Cortex Code (cf. models/README.md)
├── dashboard/streamlit_app.py        # dashboard (à utiliser après génération des marts)
└── runbook/cortex_code_prompts.md    # mode opératoire Cortex Code
```

> `models/` est volontairement vide : les modèles sont **produits par l'agent**, pas écrits à la main.

## Pré-requis

- Compte **Snowflake en région AWS** (SDK Python Snowpipe Streaming + Cortex Code = GA sur AWS). Le trial 30 j ($400) suffit.
- Python 3.9+ ; (optionnel) Docker.
- Auth **key-pair RSA** (le SDK streaming l'exige).

## Quickstart

### 1. Setup Snowflake
Édite `snowflake/00_setup.sql` (remplace `<TON_USER>`) et exécute-le dans Snowsight. Le script
crée notamment l'utilisateur de **service `SVC_CRYPTO`** (type `SERVICE`, clé RSA uniquement),
utilisé par le consumer. Génère la clé et enregistre la clé publique sur ce user de service :
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
# ALTER USER SVC_CRYPTO SET RSA_PUBLIC_KEY='...';  (cf. 00_setup.sql)
```

### 2. Lancer l'ingestion
```bash
cd ingestion
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp profile.json.example profile.json   # renseigner account/user/url + rsa_key.p8
export SYMBOLS="btcusdt,ethusdt,solusdt" DEPTH_LEVEL=20 DEPTH_SPEED=1000ms
python stream_to_snowflake.py
```
Vérifie l'arrivée des données :
```sql
SELECT COUNT(*) FROM RAW.CRYPTO.RAW_TRADES;
SELECT RECORD FROM RAW.CRYPTO.RAW_DEPTH LIMIT 1;
```

### 3. Générer les modèles avec Cortex Code
Dans Cortex Code (Snowsight), rôle `CRYPTO_PIPELINE_ROLE` :
```
$flatten-variant      # staging/intermediate/marts + tests
$realtime-marts       # vues live + Dynamic Tables
```
Puis `dbt seed` + `dbt build`. Détails : [`runbook/cortex_code_prompts.md`](./runbook/cortex_code_prompts.md).

### 4. Dashboard
Déploie `dashboard/streamlit_app.py` en **Streamlit in Snowflake** (warehouse `WH_CRYPTO_XS`).

## Coûts (FinOps)

- Snowpipe Streaming serverless ; warehouse XS `AUTO_SUSPEND=60` ; Dynamic Tables légères.
- **Resource Monitor** (cap quotidien) configuré dans `00_setup.sql`.
- Filtrer à 1–3 symboles et `DEPTH_SPEED=1000ms` pour limiter le volume.
- Estimation : ~5–12 $/mois en continu ; ~0 $ en mode démo (ingestion arrêtée).

## Sécurité

- Secrets (`profile.json`, `rsa_key.p8`) **jamais commités** (cf. `.gitignore`).
- Rôle least-privilege `CRYPTO_PIPELINE_ROLE` ; Cortex Code respecte le RBAC.
- Revue humaine du code généré par l'agent (gouvernance).

## Références

- Snowpipe Streaming SDK Python : https://docs.snowflake.com/en/user-guide/snowpipe-streaming-high-performance-overview
- Cortex Code : https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code
- dbt Projects on Snowflake + Cortex Code : https://www.snowflake.com/en/blog/building-and-deploying-dbt-projects-on-snowflake-with-cortex-code/
- Inspiration : repo `FerAou/Snow_tips/json_to_dbt` (skill `flatten-variant`).
