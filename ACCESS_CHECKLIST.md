# Checklist des accès & prérequis

Avant de lancer quoi que ce soit, voici tout ce qu'il te faut (et ce dont tu **n'as pas** besoin).

## 1. Binance (source de données) — ❌ AUCUN compte / clé / token

- Les flux WebSocket **publics** (`@trade`, `@depth`) sont **ouverts, sans authentification**.
- Pas de compte Binance, pas d'API key, pas de secret. Le consumer se connecte directement à
  `wss://stream.binance.com:9443`.
- Une clé API Binance ne servirait qu'à **passer des ordres / lire un compte privé** → hors scope.
- ⚠️ **Disponibilité géo** : `stream.binance.com` est restreint dans certains pays (ex. US → utiliser
  `stream.binance.us`). Depuis la France : OK.

## 2. Snowflake (cœur du projet) — ✅ REQUIS

- [ ] **Compte Snowflake** — trial gratuit 30 j / 400 $ de crédits : https://signup.snowflake.com
- [ ] **Région AWS** à la création (obligatoire : SDK Python Snowpipe Streaming haute-perf **et**
      Cortex Code sont GA sur AWS).
- [ ] **Édition Standard** suffit (Cortex inclus).
- [ ] Accès **ACCOUNTADMIN** (fourni d'office sur le trial) pour exécuter `snowflake/00_setup.sql`.
- [ ] **Identifiants à noter** :
  - account identifier (format `orgname-account_name`),
  - URL `https://<account_identifier>.snowflakecomputing.com`,
  - ton **username humain** (pour le setup + Cortex Code).
- [ ] **Utilisateur de service dédié** `SVC_CRYPTO` (créé par `00_setup.sql`, type `SERVICE` :
      clé RSA uniquement, ni mot de passe ni MFA) → c'est lui que le consumer utilise (`profile.json`).
- [ ] **Paire de clés RSA** = ton « token » pour le streaming (auth exigée par le SDK) :
  ```bash
  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
  openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
  ```
  puis `ALTER USER SVC_CRYPTO SET RSA_PUBLIC_KEY='...';` (cf. `00_setup.sql`).
  → **Pas de mot de passe** nécessaire pour l'ingestion.
  → Répartition des rôles : **SVC_CRYPTO** = ingestion (consumer) ; **ton user humain** = setup,
     dbt, Cortex Code.
- [ ] **Cortex Code** disponible : Snowsight → menu **Cortex Code** (ou CLI `cortex`). Vérifier
      qu'il est activé pour ton compte/rôle.

## 3. Poste / hébergement du consumer — local gratuit

- [ ] **Python 3.9+** en local (dev & démo) → suffit pour démarrer.
- [ ] (Optionnel, pour tourner **24/7**) une VM gratuite : **Oracle Cloud Always Free** ou **fly.io**
      → compte gratuit à créer seulement si tu veux du continu.

## 4. Git / versioning — optionnel

- [ ] **Compte GitHub** (gratuit) si tu veux versionner le repo.
- [ ] **PAT GitHub** (token) uniquement si tu connectes la **Git integration** de
      *dbt Projects on Snowflake* à un repo **privé**.
- [ ] **dbt** : aucun compte requis (dbt Core gratuit ; *dbt Projects on Snowflake* = exécution native).

---

## Récap « secrets / tokens » à préparer

| Secret / accès | Sert à | Obligatoire ? |
|---|---|---|
| Paire de clés RSA (`rsa_key.p8` + clé publique enregistrée) | Auth Snowpipe Streaming | ✅ Oui |
| Account identifier + username Snowflake | Connexion (profile.json) | ✅ Oui |
| Compte Snowflake (trial, région AWS) | Tout le pipeline | ✅ Oui |
| Clé API Binance | — | ❌ Non |
| Compte / token Binance | — | ❌ Non |
| VM cloud gratuite | Consumer 24/7 | ⚪ Optionnel |
| Compte GitHub + PAT | Versioning / Git integration | ⚪ Optionnel |
| Compte dbt Cloud | — | ❌ Non (natif Snowflake) |

## Coût

- Tout est couvert par le **trial Snowflake** (400 $ / 30 j) pendant le build.
- Garde-fou : **Resource Monitor** déjà configuré dans `00_setup.sql` (cap quotidien).
