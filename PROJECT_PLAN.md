# Real-Time Crypto Analytics Platform — Plan de projet

> Pipeline crypto **100 % temps réel** (latence ~5–15 s, **zéro batch**), *production-grade*, 100 % écosystème **Snowflake + dbt**, à **coût quasi nul**, avec une couche **agentic** : **Cortex Code (CoCo)** génère automatiquement la modélisation dbt à partir du JSON imbriqué.
> Source : Binance WebSocket (**trades + order book**) · Ingestion : **Snowpipe Streaming (SDK Python)** · Modélisation : **dbt Projects on Snowflake** assisté par **Cortex Code**.

---

## 1. Vision & objectif

Construire une plateforme d'intelligence de marché crypto qui ingère **en continu** les transactions et le carnet d'ordres d'un exchange public, et expose des métriques fraîches à la seconde (OHLCV, VWAP, spread, order-book imbalance, anomalies) dans un dashboard live — avec des **SLO mesurables**.

Le différenciateur : la couche de transformation (le « flatten » du JSON imbriqué en modèles dbt) est **générée et maintenue par un agent IA natif Snowflake, Cortex Code**, comme dans l'approche « agentic data engineering ».

Compétences démontrées :

- ingestion **streaming temps réel** (rows) via **Snowpipe Streaming** ;
- gestion de **semi-structuré massif** (VARIANT, tableaux imbriqués) ;
- **agentic ETL** : Cortex Code détecte le VARIANT, génère `LATERAL FLATTEN` + l'arbre dbt (staging/intermediate/marts + YAML + tests) ;
- **dbt Projects on Snowflake** (exécution dbt native) + Git integration ;
- architecture **medallion**, distinction **live (vues)** vs **historique (Dynamic Tables)** ;
- **qualité, observabilité, SLO**, **CI/CD**, **FinOps** ;
- un usage **cadré** de l'IA (idempotence, tests, revue humaine — pas de « vibe coding »).

---

## 2. Cas d'usage métier

**Persona** : analyste de marché / desk trading (vue spot temps réel : prix **et** profondeur du carnet).

**Questions répondues à la seconde :**

- OHLCV de la minute courante, VWAP, top movers (1/5/15 min) ?
- Spread bid-ask, mid-price, **microprice**, **order-book imbalance** ?
- Profondeur disponible à ±N bps ? Pic de volume / déséquilibre anormal en direct ?

---

## 3. Architecture cible

Trois idées structurantes :

1. **Runtime temps réel** : Snowpipe Streaming → Bronze en **VARIANT brut** (on garde le JSON imbriqué tel quel).
2. **Couche agentique (build-time)** : **Cortex Code** lit le VARIANT, génère les flatten + le projet dbt, compile & exécute via **dbt Projects on Snowflake**. L'agent n'est **pas** dans le chemin runtime → aucun coût IA en continu.
3. **Double chemin de service** : **live** = vues calculées à la lecture (latence = ingestion) ; **historique** = Dynamic Tables (lag 1 min).

```
  ┌───────────────────────────────┐
  │  Binance WS                    │   2 flux
  │  • @trade   (transactions)     │   WebSocket
  │  • @depth   (order book)       │
  └───────────────┬───────────────┘
                  │  Consumer Python — Snowpipe Streaming SDK (open_channel / append_rows, ~5-10s)
                  ▼
  ╔═══════════════════════════════════════ SNOWFLAKE (région AWS) ═══════════════════════════════════════╗
  ║  BRONZE (VARIANT brut, streamé)            ┌──────────────────────────────┐                           ║
  ║    raw_trades(record VARIANT)              │  Cortex Code (CoCo) — AGENT   │  build-time               ║
  ║    raw_depth(record VARIANT)  ◀────────────│  détecte VARIANT             │                           ║
  ║         │                                  │  → génère LATERAL FLATTEN    │  génère / maintient       ║
  ║         │  dbt (généré par Cortex Code)    │  → échafaude l'arbre dbt     │  la modélisation dbt      ║
  ║         ▼                                  │  → YAML + tests + run        │                           ║
  ║  SILVER (vues, flatten)                    └──────────────────────────────┘                           ║
  ║    stg_trades · stg_depth_levels (1 ligne / niveau / côté)                                            ║
  ║         │                                                                                              ║
  ║         ▼                  dbt Projects on Snowflake (exécution native) + Git integration             ║
  ║  GOLD ── LIVE (vues, calcul à la lecture) ────────┐      GOLD ── HISTO (Dynamic Tables, lag 1 min)    ║
  ║    vw_ohlcv_1min_live                              │        fct_ohlcv_1min                             ║
  ║    vw_orderbook_metrics_live (spread, imbalance)   │        fct_orderbook_snapshots                    ║
  ║    vw_market_metrics_live (movers, z-score, RSI)   │        dim_symbols (seed)                         ║
  ╚════════════════════╤══════════════════════════════╧═══════════════════════════════════════════════════╝
                       │
        ┌──────────────┼───────────────────────┐
        ▼              ▼                        ▼
   Streamlit       dbt docs / lineage      Observabilité
   (auto-refresh,  (Git / Pages)           (Elementary, SLO,
   temps réel)                              freshness, coûts)
```

---

## 4. Stack & outils (tout gratuit ou free-tier ; Cortex Code = build-time)

| Catégorie | Outil | Rôle | Coût |
|---|---|---|---|
| Source | **Binance WebSocket** (`@trade` + `@depth`) | Trades + carnet d'ordres temps réel | Gratuit, sans clé |
| Ingestion | **Snowpipe Streaming — SDK Python** (`snowflake.ingest.streaming`) | Insertion rows continue, serverless | Conso à l'usage |
| Consumer | **Python** + `websocket-client`, `snowpipe-streaming` | 2 canaux (trades, depth) → Bronze VARIANT | Gratuit |
| **Agent de modélisation** | **Cortex Code (CoCo)** — Snowsight / IDE / CLI | Détecte VARIANT, génère flatten + projet dbt, run | À l'usage (tokens), **build-time** |
| Exécution dbt | **dbt Projects on Snowflake** + **Git integration** | dbt natif dans Snowflake, versionné | Conso warehouse |
| Compute always-on | **Oracle Cloud Always Free** / fly.io / Docker local | Héberge le consumer 24/7 | Gratuit |
| Entrepôt | **Snowflake Standard (région AWS)** | Stockage + compute + Dynamic Tables + Cortex | Trial $400/30j puis pay-as-you-go |
| Historique/agrégats | **Snowflake Dynamic Tables** (lag 1 min) | Matérialisation incrémentale | Conso serverless |
| Qualité de données | **dbt tests + model contracts** (générés par CoCo) | not_null, unique, accepted_values | Gratuit |
| Observabilité | **Elementary** (OSS) + `ACCOUNT_USAGE` | Anomalies, SLO, lineage | Gratuit |
| Dashboard | **Streamlit in Snowflake** | Visualisation live (auto-refresh) | Conso XS |
| Versioning / CI (option) | **GitHub** + GitHub Actions | Repo, PR, lint, Pages | Gratuit |
| Lint / format | **SQLFluff, ruff, pre-commit** | Qualité de code | Gratuit |
| Secrets / auth | **Key-pair RSA** + secrets | Auth Snowpipe Streaming | Gratuit |

> ⚠️ **Région AWS obligatoire** : SDK Python Snowpipe Streaming (haute-perf) et la pleine disponibilité de Cortex Code sont GA sur **AWS**.

---

## 5. Modèle de données (généré par Cortex Code, revu par toi)

**Bronze — VARIANT brut, alimenté par Snowpipe Streaming**

| Table | Colonnes | Note |
|---|---|---|
| `RAW.CRYPTO.raw_trades` | `record VARIANT`, `ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()` | Message trade brut |
| `RAW.CRYPTO.raw_depth` | `record VARIANT`, `ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()` | Snapshot carnet (bids/asks = **tableaux de [prix, qty]**) |

**Silver — flatten (Cortex Code génère le `LATERAL FLATTEN`)**

- `stg_trades` (vue) : `symbol, price, quantity, trade_id, event_time, is_buyer_maker, ingest_time` (dédup `trade_id`).
- `stg_depth_levels` (vue) : **1 ligne par (symbol, side, level, price, qty)** — c'est le « tableaux dans des tableaux » aplati. `side ∈ {bid, ask}`, `level` = rang dans le carnet.

**Gold — LIVE (vues, calcul à la lecture)**

- `vw_ohlcv_1min_live` : OHLCV minute courante + fenêtre glissante (trades).
- `vw_orderbook_metrics_live` : best bid/ask, **spread (bps)**, mid, **microprice**, **order-book imbalance**, profondeur cumulée à ±N bps (depth).
- `vw_market_metrics_live` : top movers 1/5/15 min, volume z-score, volatilité, RSI.

**Gold — HISTORIQUE (Dynamic Tables, lag 1 min)**

- `fct_ohlcv_1min`, `fct_orderbook_snapshots`, `dim_symbols` (seed).

---

## 6. KPI mesurables

### 6.1 Analytics (produit)

| KPI | Définition | Modèle |
|---|---|---|
| OHLCV / VWAP | bougie minute courante, prix moyen pondéré | `vw_ohlcv_1min_live` |
| Spread (bps) | (ask − bid)/mid × 10⁴ | `vw_orderbook_metrics_live` |
| Microprice | mid pondéré par les volumes bid/ask | `vw_orderbook_metrics_live` |
| Order-book imbalance | vol_bid/(vol_bid+vol_ask) | `vw_orderbook_metrics_live` |
| Top movers | % variation 1/5/15 min | `vw_market_metrics_live` |
| Volume z-score | flag anomalie | `vw_market_metrics_live` |

### 6.2 SLO data engineering

| KPI | Cible | Source |
|---|---|---|
| **Latence end-to-end** | p95 < 15 s | `ingest_time − event_time` |
| **Fraîcheur** | warn 30 s / error 2 min | vue freshness dédiée |
| **Débit ingestion** | rows/s (2 flux) | logs + métriques streaming |
| **Couverture de tests dbt** | > 90 % | manifest dbt |
| **Réussite tests dbt** | 100 % | run_results / Elementary |
| **Uptime consumer** | > 99 % | heartbeat |
| **Coût** | < cap (resource monitor) | `WAREHOUSE_METERING_HISTORY` |
| **Part de code dbt générée par l'agent** | mesurée (métrique « narrative ») | revue Git (lignes générées vs corrigées) |

---

## 7. Budget de latence

| Étape | Cible |
|---|---|
| Binance WS → consumer | < 1 s |
| Consumer → `append_rows` (buffer) | 1–2 s |
| Commit Snowpipe Streaming | 5–10 s |
| Vue live (lecture) | ms |
| **Total p95** | **~5–15 s, zéro batch** |

> Cortex Code (génération) et les Dynamic Tables (lag 1 min) sont **hors** du chemin critique temps réel.

---

## 8. Modélisation agentique & CI/CD

**Workflow Cortex Code (cadré, pas de « vibe coding ») :**

1. Brancher Cortex Code sur la base `RAW` → l'agent **profile** `raw_trades` / `raw_depth` et détecte la structure VARIANT.
2. Prompt cadré → l'agent **génère** : `stg_*` (flatten), `int_*`, marts, `_sources.yml`, `schema.yml` (tests), `dbt_project.yml`.
3. **Revue humaine obligatoire** du SQL généré (diff Git) avant merge.
4. `dbt build` via **dbt Projects on Snowflake** → compile + tests dans un schéma CI.
5. Itération agent ↔ humain ; on versionne tout dans Git (Snowflake **Git integration**).

**Environnements** : schémas `dev` / `ci` / `prod`. **Exécution** : dbt natif (dbt Projects on Snowflake), planifié par **Snowflake Tasks** pour le redéploiement/tests ; le live ne dépend pas d'un run (vues).

**CI optionnelle GitHub Actions** : lint (`ruff`, `sqlfluff`), vérif de compilation dbt sur PR, publication des dbt docs sur GitHub Pages.

**Garde-fous IA** : revue de diff systématique, tests dbt comme filet, modèles **idempotents**, RBAC respecté (l'agent hérite du rôle least-privilege).

---

## 9. Sécurité & gouvernance

- **Key-pair RSA** pour le consumer (Snowpipe Streaming) ; secrets hors repo.
- **Rôle dédié least-privilege** (`CRYPTO_PIPELINE_ROLE`) — utilisé aussi par Cortex Code (l'agent respecte le RBAC).
- **Warehouse dédié** `WH_CRYPTO_XS` (`AUTO_SUSPEND=60`).
- **Model contracts** dbt sur les marts.
- Revue humaine du code généré par l'IA (gouvernance / qualité).

---

## 10. Coûts & FinOps

**Build** : trial **$400 / 30 j**.

**Régime permanent (24/7) :**

- **Snowpipe Streaming** serverless — filtrer à **1–3 symboles** (le depth @100ms est volumineux ; utiliser `@depth20@1000ms` pour réduire).
- **Vues live** : coût à la requête (dashboard) → suspendre hors démo.
- **Dynamic Tables** : refresh 1 min, léger.
- **Cortex Code** : coût **uniquement au build** (génération), pas en continu.
- Estimation : **~5–12 $/mois** en continu (2 flux), **~0 $** en démo.

**Garde-fous** : Resource Monitor (cap) ; peu de symboles ; `depth` à 1000 ms ; `AUTO_SUSPEND=60` ; fenêtres de vues bornées.

---

## 11. Roadmap (phases & livrables)

| Phase | Contenu | Livrable |
|---|---|---|
| **0 — Setup** | Compte Snowflake (trial, **AWS**), repo + Git integration, key-pair, Cortex Code activé | `00_setup.sql` |
| **1 — Ingestion** | Consumer 2 flux (trades + depth) → Snowpipe Streaming → Bronze VARIANT | `stream_to_snowflake.py`, Dockerfile |
| **2 — Agent (flatten)** | Cortex Code profile le VARIANT → génère `stg_*` flatten + YAML + tests | Modèles dbt générés + runbook de prompts |
| **3 — Gold live** | Vues OHLCV + order-book metrics + market metrics | `vw_*_live` |
| **4 — Gold histo** | Dynamic Tables + `dim_symbols` | `fct_*`, seed |
| **5 — Serving** | Dashboard Streamlit auto-refresh (prix + carnet) | App live |
| **6 — Observabilité** | Elementary, SLO latence, freshness, Resource Monitor | Dashboard SLO |
| **7 — CI/CD & docs** | dbt Projects on Snowflake + Tasks, dbt docs, README | Repo « prod-ready » |

---

## 12. Définition de « Done »

- Pipeline **100 % temps réel** 24/7, **p95 < 15 s** (mesuré).
- Couche flatten dbt **générée par Cortex Code**, revue et versionnée.
- **100 % tests dbt verts**, couverture > 90 %.
- Dashboard live (prix + order book).
- Coût sous le cap.
- dbt docs / lineage publié ; README + diagramme.

---

## 13. Risques & mitigations

| Risque | Mitigation |
|---|---|
| Code IA incorrect / non idempotent | Revue de diff Git + tests dbt + contrats de modèle |
| Vues live lentes (table qui grossit) | Fenêtres bornées + Dynamic Tables histo + rétention Bronze |
| Volume/coût du flux depth | 1–3 symboles, `@depth20@1000ms`, Resource Monitor |
| Déconnexions WebSocket | Reconnexion backoff + heartbeat (×2 flux) |
| Doublons | Dédup `trade_id` ; depth idempotent par `lastUpdateId` |
| Dépendance région AWS | Compte créé en région AWS dès le départ |
| Secrets exposés | Key-pair + secrets, jamais commités |

---

### Références techniques

- Cortex Code (présentation produit) : https://www.snowflake.com/en/product/features/cortex-code/
- Cortex Code (doc) : https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code
- dbt Projects on Snowflake avec Cortex Code (blog) : https://www.snowflake.com/en/blog/building-and-deploying-dbt-projects-on-snowflake-with-cortex-code/
- Agentic ETL best practices (Cortex Code) : https://www.snowflake.com/en/developers/guides/agentic-etl-best-practices/
- Snowpipe Streaming high-performance (Python, 5–10 s, GA AWS) : https://docs.snowflake.com/en/user-guide/snowpipe-streaming-high-performance-overview
- SDK Python `snowflake.ingest.streaming` : https://docs.snowflake.com/en/user-guide/snowpipe-streaming-sdk-python/reference/latest/api/snowflake/ingest/streaming/index
