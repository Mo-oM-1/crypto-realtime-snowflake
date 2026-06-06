#!/usr/bin/env python3
"""
Consumer temps réel Binance -> Snowflake (Snowpipe Streaming, architecture haute-performance).

- Lit 2 flux WebSocket Binance par symbole : @trade et @depth<level>@<speed>.
- Stocke le message JSON BRUT (imbriqué) dans une colonne VARIANT `RECORD`
  des tables RAW.CRYPTO.RAW_TRADES et RAW.CRYPTO.RAW_DEPTH.
- Le "flatten" du VARIANT est délégué à l'agent Cortex Code (skill $flatten-variant)
  côté dbt — ce script ne fait QUE l'ingestion brute.

SDK   : snowpipe-streaming
        from snowflake.ingest.streaming import StreamingIngestClient
Auth  : key-pair via profile.json (cf. profile.json.example)
Pipe  : "default pipe" auto-créé, nommé <TABLE>-STREAMING (aucun CREATE PIPE requis).
Note  : pour une colonne VARIANT, on passe un dict Python (pas une string JSON).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
import uuid

import websocket  # paquet "websocket-client"
from snowflake.ingest.streaming import StreamingIngestClient

# --- Configuration (variables d'environnement, avec valeurs par défaut) ------
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

os.environ.setdefault("SS_LOG_LEVEL", "warn")  # logs du SDK Snowpipe Streaming
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("crypto-ingest")


class TableChannel:
    """Encapsule un client + un canal Snowpipe Streaming pour une table."""

    def __init__(self, table: str):
        self.table = table
        self.pipe = f"{table}-STREAMING"  # default pipe (auto-créé par Snowflake)
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
        self._lock = threading.Lock()
        log.info("Canal ouvert: %s (pipe %s)", self.channel.channel_name, self.pipe)

    def append(self, record: dict) -> None:
        with self._lock:
            self.offset += 1
            token = str(self.offset)
        # RECORD = colonne VARIANT -> on passe un dict Python (MATCH_BY_COLUMN_NAME)
        self.channel.append_row({"RECORD": record}, token)

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

    # ---- callbacks WebSocket ----
    def on_open(self, _ws):
        self._connected.set()
        log.info("WS connecté — %d symboles (%s)", len(SYMBOLS), ", ".join(SYMBOLS))

    def on_message(self, _ws, message: str):
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return
        stream = msg.get("stream", "")
        if stream.endswith("@trade"):
            self.trades.append(msg)
        elif "@depth" in stream:
            self.depth.append(msg)

    def on_error(self, _ws, error):
        log.warning("WS error: %s", error)

    def on_close(self, _ws, code, reason):
        self._connected.clear()
        log.warning("WS fermé (code=%s reason=%s)", code, reason)

    # ---- boucle de stats (SLO / observabilité) ----
    def stats_loop(self):
        while not self._stop.wait(30):
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
        if self.ws is not None:                 # débloque run_forever() pour sortir proprement
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
        # ping_interval gère le keep-alive ; run_forever bloque jusqu'à déconnexion
        ws.run_forever(ping_interval=180, ping_timeout=10)
        if ing._stop.is_set():
            break
        log.info("Reconnexion dans %ss…", backoff)
        time.sleep(backoff)
        backoff = 1 if ing._connected.is_set() else min(backoff * 2, 30)

    log.info("Arrêt — fermeture des canaux Snowpipe Streaming…")
    ing.close()


if __name__ == "__main__":
    main()
