"""Tests unitaires du consumer d'ingestion.

On teste la LOGIQUE risquee (routage, backpressure, horodatage a la reception,
robustesse) en injectant de FAUX canaux + une horloge deterministe. Aucune
connexion Binance/Snowflake : les imports tiers du module sont lazy.
"""
import json
import queue
from datetime import datetime, timedelta

import pytest

import stream_to_snowflake as si


class FakeChannel:
    """Double de TableChannel : enregistre les append, ne touche pas Snowflake."""

    def __init__(self, name):
        self.table = name
        self.appended = []

    def append(self, record, ingest_ts):
        self.appended.append((record, ingest_ts))


FIXED_TS = datetime(2026, 6, 10, 12, 0, 0)


def make_ingestor():
    trades = FakeChannel("RAW_TRADES")
    depth = FakeChannel("RAW_DEPTH")
    ing = si.CryptoIngestor(trades=trades, depth=depth, clock=lambda: FIXED_TS)
    return ing, trades, depth


def test_build_url(monkeypatch):
    monkeypatch.setattr(si, "SYMBOLS", ["btcusdt", "ethusdt"])
    monkeypatch.setattr(si, "DEPTH_LEVEL", "20")
    monkeypatch.setattr(si, "DEPTH_SPEED", "1000ms")
    url = si.build_url()
    assert url.startswith(si.WS_BASE)
    assert "btcusdt@trade" in url
    assert "btcusdt@depth20@1000ms" in url
    assert "ethusdt@trade" in url


def test_on_message_routes_trade_to_trades_channel():
    ing, trades, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {"t": 1}}))
    target, record, _ = ing.q.get_nowait()
    assert target is trades
    assert record["stream"] == "btcusdt@trade"
    assert ing.enqueued == 1


def test_on_message_routes_depth_to_depth_channel():
    ing, _, depth = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@depth20@1000ms", "data": {}}))
    target, _, _ = ing.q.get_nowait()
    assert target is depth


def test_ingest_time_is_stamped_at_receipt():
    # Le timestamp vient de l'horloge (reception), pas pose plus tard -> SLO de latence juste.
    ing, _, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {}}))
    _, _, ts = ing.q.get_nowait()
    assert ts == FIXED_TS
    assert isinstance(ts, datetime) and ts.tzinfo is None


def test_full_queue_drops_instead_of_blocking():
    ing, _, _ = make_ingestor()
    ing.q = queue.Queue(maxsize=1)
    ing.q.put_nowait("plein")
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {}}))
    assert ing.dropped == 1
    assert ing.enqueued == 0


def test_bad_json_is_ignored():
    ing, _, _ = make_ingestor()
    ing.on_message(None, "pas du json")
    assert ing.q.qsize() == 0
    assert ing.enqueued == 0


def test_unknown_stream_is_ignored():
    ing, _, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@kline_1m", "data": {}}))
    assert ing.q.qsize() == 0


def test_drain_writes_record_and_receipt_ts_to_channel():
    # Producteur -> file -> writer : le bon record et le bon ts arrivent au bon canal.
    ing, trades, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {"t": 7}}))
    assert ing._drain_once(timeout=0.1) is True
    assert len(trades.appended) == 1
    record, ts = trades.appended[0]
    assert record["data"]["t"] == 7
    assert ts == FIXED_TS
    assert ing.processed == 1


def test_drain_returns_false_when_queue_empty():
    ing, _, _ = make_ingestor()
    assert ing._drain_once(timeout=0.05) is False


# ---- healthcheck : liveness + fraicheur ----

def test_health_starting_is_tolerated_before_first_message():
    # Au lancement, pas encore de message : sain pendant la fenetre de grace.
    ing, _, _ = make_ingestor()
    ok, info = ing.health(now=FIXED_TS)
    assert ok is True
    assert info["state"] == "starting"
    assert info["last_msg_age_s"] is None


def test_health_no_data_after_grace_window():
    # Toujours aucun message passe la fenetre de grace -> unhealthy (le socket ne livre rien).
    ing, _, _ = make_ingestor()
    ok, info = ing.health(now=FIXED_TS + timedelta(seconds=ing._grace_s + 1))
    assert ok is False
    assert info["state"] == "no_data"


def test_health_ok_when_received_and_written():
    # 'healthy' exige RECEPTION ET ECRITURE recentes -> on draine pour produire une ecriture.
    ing, _, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {}}))
    ing._drain_once(timeout=0.1)  # ecrit -> last_write_at = FIXED_TS
    ok, info = ing.health(now=FIXED_TS)
    assert ok is True
    assert info["state"] == "healthy"
    assert info["last_msg_age_s"] == 0.0
    assert info["last_write_age_s"] == 0.0


def test_health_stale_when_silence_exceeds_threshold():
    # Process vivant mais plus de message depuis trop longtemps -> zombie -> unhealthy.
    ing, _, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {}}))
    ing._drain_once(timeout=0.1)
    ok, info = ing.health(now=FIXED_TS + timedelta(seconds=ing._max_silence_s + 5))
    assert ok is False
    assert info["state"] == "stale"


def test_health_write_stalled_when_receiving_but_not_writing():
    # L'angle mort historique : on RECOIT encore mais les ecritures Snowflake sont bloquees.
    ing, _, _ = make_ingestor()
    ing.on_message(None, json.dumps({"stream": "btcusdt@trade", "data": {}}))
    ing._drain_once(timeout=0.1)  # derniere ecriture = FIXED_TS
    later = FIXED_TS + timedelta(seconds=ing._max_silence_s + 5)
    ing._last_msg_at = later       # on continue de recevoir (frais)
    ok, info = ing.health(now=later)
    assert ok is False
    assert info["state"] == "write_stalled"
    assert info["last_msg_age_s"] == 0.0           # reception OK
    assert info["last_write_age_s"] > ing._max_silence_s


# ---- resilience : recovery de canal Snowpipe Streaming ----

def test_channel_reopens_and_retries_on_invalid_channel():
    # Simule un canal qui s'invalide : le 1er append leve, le canal rouvert accepte la ligne.
    created = []

    class FakeCh:
        def __init__(self, fail):
            self.fail, self.rows, self.channel_name = fail, [], "ch"

        def append_row(self, row, offset):
            if self.fail:
                raise RuntimeError("InvalidChannelError: invalid state")
            self.rows.append(row)

        def close(self):
            pass

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def open_channel(self, name):
            ch = FakeCh(fail=(len(created) == 0))  # seul le 1er canal echoue
            created.append(ch)
            return (ch,)

        def close(self):
            pass

    tc = si.TableChannel("RAW_TRADES", client_factory=FakeClient)
    tc.append({"k": 1}, FIXED_TS)

    assert tc.reopens == 1                  # le canal a ete rouvert une fois
    assert len(created) == 2                # canal initial + canal rouvert
    assert created[1].rows == [{"RECORD": {"k": 1}, "INGEST_TIME": FIXED_TS}]  # ecrit apres reopen


def test_circuit_breaker_stops_reopen_storm_under_sustained_failure():
    # Panne soutenue : TOUS les canaux echouent -> on ne doit PAS rouvrir a chaque ligne.
    created = []

    class FakeCh:
        channel_name = "ch"

        def append_row(self, row, offset):
            raise RuntimeError("InvalidChannelError: invalid state")

        def close(self):
            pass

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def open_channel(self, name):
            ch = FakeCh()
            created.append(ch)
            return (ch,)

        def close(self):
            pass

    tc = si.TableChannel("RAW_TRADES", client_factory=FakeClient)
    # 1re ligne : 1 reouverture-sonde, retry echoue -> backoff arme
    with pytest.raises(Exception):
        tc.append({"k": 1}, FIXED_TS)
    n_after_first = len(created)            # initial + 1 reopen = 2
    # lignes suivantes pendant le backoff : echec rapide, AUCUNE nouvelle reouverture
    for _ in range(50):
        with pytest.raises(Exception):
            tc.append({"k": 2}, FIXED_TS)
    assert len(created) == n_after_first    # pas de reopen-storm (circuit ouvert)
    assert tc.reopens == 1


def test_transient_error_does_not_reopen_channel():
    # Un blip reseau (pas une invalidation de canal) ne doit PAS jeter le canal.
    created = []

    class FakeCh:
        channel_name = "ch"

        def append_row(self, row, offset):
            raise RuntimeError("temporary network blip")  # pas un 'invalid channel'

        def close(self):
            pass

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def open_channel(self, name):
            ch = FakeCh()
            created.append(ch)
            return (ch,)

        def close(self):
            pass

    tc = si.TableChannel("RAW_TRADES", client_factory=FakeClient)
    with pytest.raises(Exception):
        tc.append({"k": 1}, FIXED_TS)
    assert tc.reopens == 0                  # canal preserve
    assert len(created) == 1


def test_circuit_breaker_when_reopen_itself_fails():
    # Panne ou open_channel() ECHOUE (Snowflake injoignable) : le backoff doit quand meme
    # s'armer -> pas de storm de reopen (le cas le plus probable d'une vraie panne).
    created = []

    class FailingClient:
        def __init__(self, **kwargs):
            created.append(1)
            if len(created) > 1:            # le 1er client s'ouvre ; les reopen echouent
                raise RuntimeError("InvalidChannelError: cannot open channel")

        def open_channel(self, name):
            class Ch:
                channel_name = "ch"

                def append_row(self, row, off):
                    raise RuntimeError("InvalidChannelError: invalid state")

                def close(self):
                    pass

            return (Ch(),)

        def close(self):
            pass

    tc = si.TableChannel("RAW_TRADES", client_factory=FailingClient)
    for _ in range(50):
        with pytest.raises(Exception):
            tc.append({"k": 1}, FIXED_TS)
    assert tc.reopens <= 1                  # 1 sonde puis backoff, pas de storm cote open_channel
