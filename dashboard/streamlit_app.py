"""Dashboard Streamlit (in Snowflake) - crypto temps reel + surveillance.

Marts dbt lus (generes par Cortex Code) :
    ANALYTICS.PUBLIC_MARTS.VW_OHLCV_1MIN_LIVE
    ANALYTICS.PUBLIC_MARTS.VW_ORDERBOOK_METRICS_LIVE
    ANALYTICS.PUBLIC_MARTS.VW_MARKET_METRICS_LIVE
    ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES (surveillance Cortex ML)

Deploiement : Snowsight -> Streamlit -> New app (warehouse WH_CRYPTO_XS).
Necessite Streamlit >= 1.37 (st.fragment / run_every).
"""
import logging

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

logger = logging.getLogger("crypto-dashboard")
session = get_active_session()
st.set_page_config(page_title="Crypto Real-Time", layout="wide")

MARTS = "ANALYTICS.PUBLIC_MARTS"
STG = "ANALYTICS.PUBLIC_STAGING"
MON = "ANALYTICS.MONITORING"
UP, DOWN = "#26a69a", "#ef5350"   # vert / rouge bougies


# --- Acces donnees : caches 10 s (1er poste de cout), requetes PARAMETREES ----
# _session prefixe par '_' => non hashe par le cache (le symbole est la cle de cache).
@st.cache_data(ttl=10)
def load_symbols(_session):
    return [r[0] for r in _session.sql(
        f"SELECT DISTINCT symbol FROM {MARTS}.VW_OHLCV_1MIN_LIVE ORDER BY symbol"
    ).collect()]


@st.cache_data(ttl=10)
def load_ohlcv(_session, symbol):
    return _session.sql(
        f"SELECT minute, open, high, low, close, volume, vwap "
        f"FROM {MARTS}.VW_OHLCV_1MIN_LIVE WHERE symbol = ? ORDER BY minute",
        params=[symbol],
    ).to_pandas()


@st.cache_data(ttl=10)
def load_orderbook(_session, symbol):
    return _session.sql(
        f"SELECT mid, spread_bps, imbalance, microprice "
        f"FROM {MARTS}.VW_ORDERBOOK_METRICS_LIVE WHERE symbol = ?",
        params=[symbol],
    ).to_pandas()


@st.cache_data(ttl=10)
def load_metrics(_session, symbol):
    return _session.sql(
        f"SELECT minute, rsi_14, realized_volatility, price_change_pct_5min "
        f"FROM {MARTS}.VW_MARKET_METRICS_LIVE WHERE symbol = ? ORDER BY minute",
        params=[symbol],
    ).to_pandas()


@st.cache_data(ttl=10)
def load_movers(_session):
    return _session.sql(f"""
        SELECT symbol, price_change_pct_5min, rsi_14, is_volume_anomaly
        FROM {MARTS}.VW_MARKET_METRICS_LIVE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY minute DESC) = 1
        ORDER BY ABS(price_change_pct_5min) DESC
    """).to_pandas()


@st.cache_data(ttl=10)
def load_slo(_session):
    return _session.sql(f"""
        SELECT ROUND(AVG(DATEDIFF('ms', traded_at, ingest_time)) / 1000.0, 3) AS avg_s,
               ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                     ORDER BY DATEDIFF('ms', traded_at, ingest_time)) / 1000.0, 3) AS p95_s,
               ROUND(COUNT(*) / 300.0, 1) AS trades_per_s,
               -- Fraicheur lue DIRECT sur RAW (un MAX(ingest_time) n'a pas besoin du dedup
               -- QUALIFY de stg_trades) -> moins cher.
               (SELECT DATEDIFF('second', MAX(ingest_time), SYSDATE())
                FROM RAW.CRYPTO.RAW_TRADES) AS freshness_s
        FROM {STG}.STG_TRADES
        WHERE ingest_time >= DATEADD('minute', -5, SYSDATE())
    """).to_pandas()


@st.cache_data(ttl=10)
def load_events(_session):
    return _session.sql(
        f"SELECT checked_at, metric, value, status FROM {MON}.pipeline_log "
        f"ORDER BY checked_at DESC LIMIT 10"
    ).to_pandas()


@st.cache_data(ttl=10)
def load_anomalies(_session, symbol):
    return _session.sql(
        f"SELECT minute, volume, lower_bound, upper_bound, is_anomaly, distance "
        f"FROM {MON}.MART_VOLUME_ANOMALIES "
        f"WHERE symbol = ? AND minute >= DATEADD('hour', -2, SYSDATE()) ORDER BY minute",
        params=[symbol],
    ).to_pandas()


@st.cache_data(ttl=10)
def load_cvd(_session, symbol):
    # CVD = Cumulative Volume Delta. Taker = l'agresseur :
    #   is_buyer_market_maker = TRUE  -> l'acheteur est passif -> taker SELL
    #   is_buyer_market_maker = FALSE -> l'acheteur est l'agresseur -> taker BUY
    return _session.sql(f"""
        with per_min as (
            select time_slice(traded_at, 1, 'MINUTE') as minute,
                   sum(case when not is_buyer_market_maker then quantity else 0 end) as taker_buy,
                   sum(case when     is_buyer_market_maker then quantity else 0 end) as taker_sell
            from {STG}.STG_TRADES
            where symbol = ? and traded_at >= dateadd('minute', -120, sysdate())
            group by 1
        )
        select minute, taker_buy, taker_sell,
               (taker_buy - taker_sell)                       as delta,
               sum(taker_buy - taker_sell) over (order by minute) as cvd
        from per_min
        order by minute
    """, params=[symbol]).to_pandas()


@st.cache_data(ttl=10)
def load_orderbook_levels(_session, symbol):
    # Dernier snapshot du carnet (max last_update_id) -> niveaux bid/ask.
    return _session.sql(f"""
        with latest as (
            select side, price, qty, level
            from {STG}.STG_DEPTH_LEVELS
            where symbol = ? and ingest_time >= dateadd('minute', -120, sysdate())
            qualify last_update_id = max(last_update_id) over (partition by symbol)
        )
        select side, level, price::float as price, qty::float as qty
        from latest
        order by price
    """, params=[symbol]).to_pandas()


@st.cache_data(ttl=10)
def load_cross_perf(_session):
    # Performance normalisee base 100 par symbole, sur la fenetre live.
    return _session.sql(f"""
        with b as (
            select symbol, minute, close,
                   first_value(close) over (partition by symbol order by minute) as first_close
            from {MARTS}.VW_OHLCV_1MIN_LIVE
        )
        select symbol, minute, (close / nullif(first_close, 0) * 100)::float as perf_100
        from b
        order by minute
    """).to_pandas()


# --- En-tete (hors fragment : la selection ne doit pas etre auto-rafraichie) --
st.title("Crypto Real-Time - Snowflake + dbt")
symbols = load_symbols(session)
st.selectbox("Symbole", symbols or ["BTCUSDT"], key="symbol")
st.caption("Rafraichissement auto toutes les 10 s (vues live calculees a la lecture, en cache 10 s).")


@st.fragment(run_every=10)
def live_dashboard():
    symbol = st.session_state.get("symbol") or (symbols[0] if symbols else "BTCUSDT")

    col1, col2 = st.columns([2, 1])

    # --- Prix : bougies OHLC + volume colore ---
    with col1:
        st.subheader(f"OHLCV 1 min - {symbol}")
        ohlcv = load_ohlcv(session, symbol)
        if ohlcv.empty:
            st.info("Pas de donnees recentes pour ce symbole.")
        else:
            up_down = alt.condition("datum.OPEN <= datum.CLOSE", alt.value(UP), alt.value(DOWN))
            base = alt.Chart(ohlcv).encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                color=up_down,
            )
            wick = base.mark_rule().encode(
                y=alt.Y("LOW:Q", scale=alt.Scale(zero=False), title="prix"), y2="HIGH:Q")
            body = base.mark_bar(size=5).encode(y="OPEN:Q", y2="CLOSE:Q")
            st.altair_chart((wick + body).properties(height=320), width="stretch")

            vol = alt.Chart(ohlcv).mark_bar().encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                y=alt.Y("VOLUME:Q", title="volume"),
                color=up_down,
            ).properties(height=130)
            st.altair_chart(vol, width="stretch")

    # --- Order book + indicateurs (RSI / volatilite, deja calcules par le mart) ---
    with col2:
        st.subheader("Order book (live)")
        ob = load_orderbook(session, symbol)
        if not ob.empty:
            r = ob.iloc[0]
            st.metric("Mid", f"{r['MID']:.2f}")
            st.metric("Spread (bps)", f"{r['SPREAD_BPS']:.2f}")
            st.metric("Imbalance", f"{r['IMBALANCE']:.2%}")
            st.metric("Microprice", f"{r['MICROPRICE']:.2f}")

        st.subheader("Indicateurs")
        metrics = load_metrics(session, symbol)
        if metrics.empty:
            st.info("Indicateurs indisponibles.")
        else:
            last = metrics.iloc[-1]
            rsi, vol_r, chg = last["RSI_14"], last["REALIZED_VOLATILITY"], last["PRICE_CHANGE_PCT_5MIN"]
            st.metric("RSI 14", "warm-up" if pd.isna(rsi) else f"{rsi:.1f}")
            st.metric("Volatilite realisee", "-" if pd.isna(vol_r) else f"{vol_r:.4f}")
            st.metric("Variation 5 min", "-" if pd.isna(chg) else f"{chg:+.2f}%")

    # --- RSI dans le temps avec bandes 30 / 70 ---
    if not metrics.empty and metrics["RSI_14"].notna().any():
        rsi_line = alt.Chart(metrics).mark_line().encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("RSI_14:Q", scale=alt.Scale(domain=[0, 100]), title="RSI 14"),
        )
        b70 = alt.Chart(metrics).mark_rule(strokeDash=[4, 4], color="gray").encode(y=alt.datum(70))
        b30 = alt.Chart(metrics).mark_rule(strokeDash=[4, 4], color="gray").encode(y=alt.datum(30))
        st.altair_chart((rsi_line + b70 + b30).properties(height=180), width="stretch")

    # --- Flux taker : CVD (Cumulative Volume Delta) ---
    cvd = load_cvd(session, symbol)
    if not cvd.empty:
        st.subheader("Flux taker - CVD (Cumulative Volume Delta)")
        st.caption("CVD > 0 = pression acheteuse nette depuis le debut de la fenetre.")
        cvd_area = alt.Chart(cvd).mark_area(opacity=0.4, line=True, color="#42a5f5").encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("CVD:Q", title="CVD (cumul buy - sell)"),
        )
        zero = alt.Chart(cvd).mark_rule(strokeDash=[4, 4], color="gray").encode(y=alt.datum(0))
        st.altair_chart((cvd_area + zero).properties(height=200), width="stretch")

        delta_bars = alt.Chart(cvd).mark_bar().encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("DELTA:Q", title="delta / min"),
            color=alt.condition("datum.DELTA >= 0", alt.value(UP), alt.value(DOWN)),
        ).properties(height=120)
        st.altair_chart(delta_bars, width="stretch")

    # --- Carnet d'ordres (ladder) ---
    levels = load_orderbook_levels(session, symbol)
    if not levels.empty:
        st.subheader("Carnet d'ordres - ladder (dernier snapshot)")
        ladder = alt.Chart(levels).mark_bar().encode(
            y=alt.Y("PRICE:O", sort="descending", title="prix"),
            x=alt.X("QTY:Q", title="quantite"),
            color=alt.Color("SIDE:N", title=None,
                            scale=alt.Scale(domain=["bid", "ask"], range=[UP, DOWN])),
        ).properties(height=420)
        st.altair_chart(ladder, width="stretch")

    # --- Cross-symbole : perf base 100 + correlation des log-returns ---
    xperf = load_cross_perf(session)
    if not xperf.empty:
        st.divider()
        st.subheader("Cross-symbole - performance normalisee (base 100)")
        lines = alt.Chart(xperf).mark_line().encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("PERF_100:Q", scale=alt.Scale(zero=False), title="base 100"),
            color=alt.Color("SYMBOL:N", title=None),
        ).properties(height=240)
        st.altair_chart(lines, width="stretch")

        piv = xperf.pivot(index="MINUTE", columns="SYMBOL", values="PERF_100")
        corr = np.log(piv / piv.shift(1)).corr().round(2)
        if not corr.empty:
            st.caption("Correlation des log-returns")
            st.dataframe(corr, width="stretch")

    # --- Top movers ---
    st.subheader("Top movers (5 min)")
    st.dataframe(load_movers(session), width="stretch")

    # --- Surveillance : anomalies de volume (Cortex ML) ---
    st.divider()
    st.subheader("Surveillance - anomalies de volume (modele Cortex ML)")
    try:
        anom = load_anomalies(session, symbol)
        if anom.empty:
            st.info("Aucune donnee scoree (couche ML non deployee ou pas de scoring recent).")
        else:
            flagged = anom[anom["IS_ANOMALY"]]
            st.metric("Anomalies (2 h)", len(flagged))
            band = alt.Chart(anom).mark_area(opacity=0.15).encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                y=alt.Y("LOWER_BOUND:Q", title="volume"), y2="UPPER_BOUND:Q")
            line = alt.Chart(anom).mark_line().encode(
                x="MINUTE:T", y=alt.Y("VOLUME:Q", scale=alt.Scale(zero=False)))
            pts = alt.Chart(flagged).mark_point(color="red", size=90, filled=True).encode(
                x="MINUTE:T", y="VOLUME:Q")
            st.altair_chart((band + line + pts).properties(height=240), width="stretch")
    except Exception as exc:  # couche optionnelle : on logue la vraie erreur, on n'echoue pas l'app
        logger.warning("panneau anomalies indisponible: %s", exc)
        st.info("Couche surveillance non deployee (snowflake/07_ml_anomaly.sql).")

    # --- SLO / sante du pipeline ---
    st.divider()
    st.subheader("SLO / sante du pipeline")
    try:
        slo = load_slo(session)
        if not slo.empty:
            s = slo.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Latence moy (s)", "-" if pd.isna(s["AVG_S"]) else s["AVG_S"])
            c2.metric("Latence p95 (s)", "-" if pd.isna(s["P95_S"]) else s["P95_S"])
            c3.metric("Trades / s", "-" if pd.isna(s["TRADES_PER_S"]) else s["TRADES_PER_S"])
            fr = s["FRESHNESS_S"]
            c4.metric("Fraicheur (s)", "-" if pd.isna(fr) else int(fr))
            if not pd.isna(fr) and int(fr) > 120:
                st.warning("Donnees obsoletes (> 120 s) - le consumer tourne-t-il ?")
    except Exception as exc:
        logger.exception("panneau SLO indisponible")
        st.warning(f"SLO indisponible: {exc}")

    try:
        events = load_events(session)
        if not events.empty:
            st.caption("Derniers evenements (alertes fraicheur / anomalies / tests)")
            st.dataframe(events, width="stretch")
    except Exception as exc:
        logger.warning("journal pipeline_log indisponible: %s", exc)


live_dashboard()
