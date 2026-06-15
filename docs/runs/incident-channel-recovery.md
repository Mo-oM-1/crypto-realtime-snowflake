# Incident — canal Snowpipe invalide : RAW figé + disque saturé

Incident de prod réel (2026-06-15) sur le consumer hébergé 24/7. Détecté, diagnostiqué,
corrigé, puis **outillé pour ne plus se reproduire**. Montre la boucle d'ingénierie complète,
pas juste « ça marche ».

## Symptôme

Dashboard vide ; la page *Santé pipeline* affiche **« Données obsolètes (> 120 s) »**.
Pourtant le consumer tournait depuis ~26 h et **recevait** bien Binance.

## Diagnostic (par les compteurs internes, puis les logs)

`GET /healthz` révèle l'anomalie — la **réception** est OK mais les chiffres ne collent pas :

```json
{ "connected": true, "last_msg_age_s": 0.0,
  "enqueued": 7889856, "processed": 784184, "dropped": 0, "queue_size": 0 }
```

`enqueued` (7,9 M) ≫ `processed` (784 K) avec une **file vide** et `dropped: 0` → le writer
**vide la file mais les écritures Snowflake échouent** (l'exception retire la ligne sans
incrémenter `processed`). Les logs confirment la cause exacte :

```
WARNING append RAW_TRADES: InvalidChannelError: Channel ... is in an invalid state.
Please close and re-open the channel ... (HTTP 409 Conflict)
```

## Cause racine (double)

1. **Canal Snowpipe Streaming invalidé** (reset serveur / token après un long run). Le consumer
   ouvrait le canal **une seule fois** au démarrage et, une fois invalide, **réessayait en boucle
   sur le canal mort** — chaque écriture échouait, RAW restait figé.
2. **Log non borné** : chaque échec écrivait un warning (plusieurs/seconde pendant des heures) →
   `/var/log/syslog` a gonflé à **2,5 Go** et **saturé le disque** (8 Go) — ce qui aurait fini par
   tuer la VM.

Effet pervers : `/healthz` répondait `healthy` car il ne vérifiait que la **réception** (Binance),
pas le **succès des écritures** (Snowflake). **Angle mort.**

## Correctifs (3, testés)

1. **Self-heal du canal** — sur échec d'`append`, le `TableChannel` **ferme et rouvre** le canal
   puis **retente** une fois. Les doublons éventuels sont absorbés par le **dedup downstream**
   (`stg_trades` déduplique par `trade_id`) → ingestion idempotente.
2. **Healthcheck honnête** — suivi de `last_write_at` ; `/healthz` passe en **`write_stalled`
   (503)** si on reçoit mais qu'on n'écrit plus. Expose `last_write_age_s`, `write_errors`,
   `channel_reopens`.
3. **Log borné** — l'erreur d'écriture est loggée **1 fois / 1000** (comme `dropped`) : une panne
   Snowflake prolongée ne peut plus saturer le disque.

Tests ajoutés (`ingestion/tests/test_consumer.py`) : `write_stalled` quand on reçoit sans écrire,
et reopen+retry du canal (via une `client_factory` injectable). **15 tests verts.**

## Vérification (après déploiement)

```json
{ "state": "healthy", "last_msg_age_s": 0.2, "last_write_age_s": 0.2,
  "enqueued": 2240, "processed": 2240, "write_errors": 0, "channel_reopens": 0 }
```
`enqueued == processed`, écriture fraîche → data qui coule de nouveau vers RAW.

## Leçons

- **Un healthcheck doit refléter la *sortie*, pas seulement l'*entrée*.** « Je reçois » ≠ « je
  livre ». Le `/healthz` initial mentait par omission.
- **Tout log dans une boucle chaude doit être borné** — sinon une panne se transforme en
  saturation disque.
- **Les compteurs internes (`enqueued`/`processed`) sont le premier outil de diagnostic** : ils
  ont pointé la cause avant même les logs.
- Une ressource externe (canal streaming) **peut s'invalider** : le client doit savoir se
  reconnecter, pas juste réessayer aveuglément.
