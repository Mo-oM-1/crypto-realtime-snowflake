#!/usr/bin/env python3
"""
Consumer temps reel Binance -> Snowflake (Snowpipe Streaming, architecture haute-performance).

- Lit 2 flux WebSocket Binance par symbole : @trade et @depth<level>@<speed>.
- Stocke le message JSON BRUT (imbrique) dans une colonne VARIANT `RECORD`
  des tables RAW.CRYPTO.RAW_TRADES et RAW.CRYPTO.RAW_DEPTH.
- Le "flatten" du VARIANT est delegue a l'agent Cortex Code (skill $flatten-variant)
  cote dbt : ce script ne fait QUE l'ingestion brute.

Architecture (resilience / backpressure)
----------------------------------------
Producteur / consommateur decouple par une file bornee :
  thread WebSocket (on_message)  --put_nowait-->  queue.Queue(maxsize)  --get-->  thread writer  --append_row-->  Snowflake

Le thread qui ecoute Binance ne fait JAMAIS d'I/O Snowflake : il parse, estampille
l'heure de reception, et pousse dans la file. Si l'ecriture Snowflake ralentit, la file
absorbe la rafale ; en cas de saturation, la perte est EXPLICITE et observable (compteur
`dropped`) au lieu d'un drop silencieux du buffer TCP qui ferait deconnecter Binance.

SDK   : snowpipe-streaming
        from snowflake.ingest.streaming import StreamingIngestClient
Auth  : key-pair via profile.json (cf. profile.json.example)
Pipe  : "default pipe" auto-cree, nomme <TABLE>-STREAMING (aucun CREATE PIPE requis).
Note  : pour une colonne VARIANT, on passe un dict Python (pas une string JSON).
FinOps: la cadence de flush reseau vers Snowflake est gouvernee par le SDK (MAX_CLIENT_LAG) ;
        le micro-batch applicatif ci-dessous borne seulement le travail par iteration et
        n'ajoute pas de latence a faible charge.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import signal
import threading
import time
import uuid
from datetime import datetime, timezone

import websocket  # paquet "websocket-client"
from snowflake.ingest.streaming import StreamingIngestClient

# --- Configuration (variables d'environnement, avec valeurs par defaut) ------
SYMBOLS = [s.strip().lower()
           for s in os.environ.get("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")
           if s.strip()]
DEPTH_LEVEL = os.environ.get("DEPTH_LEVEL", "20")       # 5, 10 ou 20
DEPTH_SPEED = os.environ.get("DEPTH_SPEED", "1000ms")   # 100ms ou 1000ms (1000ms = moins de volume = moins cher)

DATABASE     = os.environ.get("SNOWFLAKE_DATABASE", "RAW")
SCHEMA       = os.environ.get("SNOWFLAKE_SCHEMA", "CRYPTO")
PROFILE_JSON = os.environ.get("SNOWFLAKE_PROFILE_JSON", "profile.json")

TRADES_TABLE = "RAW_TRADES"
DEPTH_TABLE  = "RAW_DEPTH"

WS_BASE = "wss://stream.binance.com:9443/stream?streams="

# --- Backpressure / micro-batch (reglables sans toucher au code) -------------
QUEUE_MAXSIZE  = int(os.environ.get("QUEUE_MAXSIZE", "100000"))    # borne memoire ; au-dela -> drop observable
BATCH_MAX_ROWS = int(os.environ.get("BATCH_MAX_ROWS", "5000"))     # taille max d'un micro-batch draine
BATCH_MAX_SECS = float(os.environ.get("BATCH_MAX_SECONDS", "1.0")) # borne temps d'un micro-batch

os.environ.setdefault("SS_LOG_LEVEL", "warn")  # logs du SDK Snowpipe Streaming
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("crypto-ingest")


class TableChannel:
    """Encapsule un client + un canal Snowpipe Streaming pour une table.

    `append` est appele UNIQUEMENT par le thread writer (ecrivain unique) :
    pas de verrou necessaire pour l'offset.
    """

    def __init__(self, table: str):
        self.table = table
        self.pipe = f"{table}-STREAMING"  # default pipe (auto-cree par Snowflake)
        self.client = StreamingIngestClient(
            client_name=f"CRYPTO_{table}_{uuid.uuid4()}",
            db_name=DATABASE,
            schema_name=SCHEMA,
            pipe_name=self.pipe,
            profile_json=PROFILE_JSON,
        )
        # open_channel renvoie un tuple ; le canal est en position [0]
        self.channel = self.client.open_channel(f"CH_{table}_{uuid.uuid4()}")[0]
        self.offset = 0
        log.info("Canal ouvert: %s (pipe %s)", self.channel.channel_name, self.pipe)

    def append(self, record: dict, ingest_ts: datetime) -> None:
        self.offset += 1
        # RECORD = colonne VARIANT (dict Python). INGEST_TIME = heure de RECEPTION,
        # estampillee dans on_message (avant la file) : le DEFAULT CURRENT_TIMESTAMP()
        # de la table s'evalue par batch (valeur uniforme, inutilisable pour la latence),
        # et estampiller ici (apres la file) reintroduirait la latence d'attente en queue.
        self.channel.append_row(
            {"RECORD": record, "INGEST_TIME": ingest_ts},
            str(self.offset),
        )

    def status(self):
        return self.channel.get_channel_status()

    def close(self) -> None:
        try:
            self.channel.close()
        finally:
            self.client.close()


class CryptoIngestor:
    def __init__(self):
        self.trades = TableChannel(TRADES_TABLE)
        self.depth = TableChannel(DEPTH_TABLE)
        self._stop = threading.Event()
        self._connected = threading.Event()
        self.ws = None
        # file bornee de decouplage WebSocket -> writer
        self.q: "queue.Queue[tuple[TableChannel, dict, datetime]]" = queue.Queue(maxsize=QUEUE_MAXSIZE)
        # compteurs (chaque compteur n'est ecrit que par UN thread -> pas de verrou)
        self.enqueued = 0   # ecrit par le thread WebSocket
        self.dropped = 0    # ecrit par le thread WebSocket
        self.processed = 0  # ecrit par le thread writer

    # ---- callbacks WebSocket (thread de lecture du socket : zero I/O Snowflake) ----
    def on_open(self, _ws):
        self._connected.set()
        log.info("WS connecte - %d symboles (%s)", len(SYMBOLS), ", ".join(SYMBOLS))

    def on_message(self, _ws, message: str):
        # Heure de RECEPTION, estampillee AVANT la mise en file (cf. note dans append()).
        ingest_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return
        stream = msg.get("stream", "")
        if stream.endswith("@trade"):
            target = self.trades
        elif "@depth" in stream:
            target = self.depth
        else:
            return
        try:
            self.q.put_nowait((target, msg, ingest_ts))
            self.enqueued += 1
        except queue.Full:
            # Backpressure : file saturee -> perte EXPLICITE et comptee (pas un drop TCP silencieux).
            self.dropped += 1
            if self.dropped % 1000 == 1:
                log.warning("File pleine (maxsize=%d) - %d messages droppes cumules",
                            QUEUE_MAXSIZE, self.dropped)

    def on_error(self, _ws, error):
        log.warning("WS error: %s", error)

    def on_close(self, _ws, code, reason):
        self._connected.clear()
        log.warning("WS ferme (code=%s reason=%s)", code, reason)

    # ---- thread writer : draine la file en micro-batch et ecrit dans Snowflake ----
    def writer_loop(self):
        while True:
            try:
                first = self.q.get(timeout=0.5)
            except queue.Empty:
                if self._stop.is_set():
                    break          # stop demande ET file vide -> on sort
                continue
            # Micro-batch adaptatif : coalesce ce qui est DEJA disponible, sans attendre.
            # A faible charge -> batch de 1 (latence minimale) ; en rafale -> jusqu'a BATCH_MAX_ROWS.
            batch = [first]
            deadline = time.monotonic() + BATCH_MAX_SECS
            while len(batch) < BATCH_MAX_ROWS and time.monotonic() < deadline:
                try:
                    batch.append(self.q.get_nowait())
                except queue.Empty:
                    break
            for target, record, ts in batch:
                try:
                    target.append(record, ts)
                    self.processed += 1
                except Exception as e:  # pragma: no cover
                    log.warning("append %s: %s", target.table, e)

    # ---- boucle de stats (SLO / observabilite) ----
    def stats_loop(self):
        while not self._stop.wait(30):
            log.info("file: depth=%d enqueued=%d processed=%d dropped=%d",
                     self.q.qsize(), self.enqueued, self.processed, self.dropped)
            for tc in (self.trades, self.depth):
                try:
                    s = tc.status()
                    log.info("[%s] inserted=%s errors=%s committed_offset=%s",
                             tc.table, s.rows_inserted_count, s.rows_error_count,
                             s.latest_committed_offset_token)
                except Exception as e:  # pragma: no cover
                    log.debug("status %s: %s", tc.table, e)

    def stop(self, *_):
        self._stop.set()
        if self.ws is not None:                 # debloque run_forever() pour sortir proprement
            try:
                self.ws.close()
            except Exception:
                pass

    def close(self):
        self.trades.close()
        self.depth.close()


def build_url() -> str:
    streams = []
    for s in SYMBOLS:
        streams.append(f"{s}@trade")
        streams.append(f"{s}@depth{DEPTH_LEVEL}@{DEPTH_SPEED}")
    return WS_BASE + "/".join(streams)


def main():
    ing = CryptoIngestor()
    signal.signal(signal.SIGINT, ing.stop)
    signal.signal(signal.SIGTERM, ing.stop)

    # writer NON-daemon : on le join a l'arret pour drainer la file avant de fermer les canaux
    writer = threading.Thread(target=ing.writer_loop, name="ss-writer")
    writer.start()
    threading.Thread(target=ing.stats_loop, daemon=True).start()

    url = build_url()
    log.info("Connexion: %s", url)

    backoff = 1
    while not ing._stop.is_set():
        ws = websocket.WebSocketApp(
            url,
            on_open=ing.on_open,
            on_message=ing.on_message,
            on_error=ing.on_error,
            on_close=ing.on_close,
        )
        ing.ws = ws  # pour que stop() (Ctrl+C) puisse fermer la connexion
        # ping_interval gere le keep-alive ; run_forever bloque jusqu'a deconnexion
        ws.run_forever(ping_interval=180, ping_timeout=10)
        if ing._stop.is_set():
            break
        log.info("Reconnexion dans %ss...", backoff)
        time.sleep(backoff)
        backoff = 1 if ing._connected.is_set() else min(backoff * 2, 30)

    log.info("Arret - drainage de la file puis fermeture des canaux Snowpipe Streaming...")
    writer.join(timeout=30)  # laisse le writer vider la file restante
    ing.close()


if __name__ == "__main__":
    main()
