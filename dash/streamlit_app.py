"""Dashboard Streamlit (in Snowflake) - crypto temps reel + surveillance.

App MULTIPAGE (st.navigation / st.Page) : une page par theme, sidebar a gauche,
selecteur de symbole partage (sidebar) applique a toutes les pages.

Marts dbt lus :
    ANALYTICS.PUBLIC_MARTS.VW_OHLCV_1MIN_LIVE / VW_ORDERBOOK_METRICS_LIVE / VW_MARKET_METRICS_LIVE
    ANALYTICS.MONITORING.MART_VOLUME_ANOMALIES (surveillance Cortex ML)
Necessite Streamlit >= 1.36 (st.navigation) et >= 1.37 (st.fragment / run_every).
"""
import logging
from datetime import datetime, timezone

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

logger = logging.getLogger("crypto-dashboard")
session = get_active_session()
st.set_page_config(page_title="Crypto Real-Time", page_icon="📈", layout="wide")

MARTS = "ANALYTICS.PUBLIC_MARTS"
STG = "ANALYTICS.PUBLIC_STAGING"
MON = "ANALYTICS.MONITORING"
UP, DOWN = "#16a34a", "#ef4444"   # vert / rouge (lisibles sur fond clair)
ACCENT = "#1e3a8a"                # navy, accorde au theme
GRID, AXIS, REF = "#e9eef5", "#64748b", "#cbd5e1"


def _style(chart, height=300):
    """Style commun des graphes : axes epures, pas de cadre, grille claire, legende en haut."""
    return (
        chart.properties(height=height)
        .configure_view(stroke=None)
        .configure_axis(
            grid=True, gridColor=GRID, domain=False, tickColor=GRID,
            labelColor=AXIS, titleColor=AXIS, labelFontSize=11,
            titleFontSize=11, titleFontWeight="normal",
        )
        .configure_legend(labelColor=AXIS, titleColor=AXIS, labelFontSize=11, orient="top")
    )


def _tmin(title="min"):
    return alt.Tooltip("MINUTE:T", format="%H:%M", title=title)


# --- Acces donnees : caches 10 s, requetes PARAMETREES (_session non hashe) -----
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
    #   is_buyer_market_maker = TRUE  -> acheteur passif -> taker SELL ; FALSE -> taker BUY.
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
               (taker_buy - taker_sell)                           as delta,
               sum(taker_buy - taker_sell) over (order by minute) as cvd
        from per_min order by minute
    """, params=[symbol]).to_pandas()


@st.cache_data(ttl=10)
def load_orderbook_levels(_session, symbol):
    return _session.sql(f"""
        with latest as (
            select side, price, qty, level
            from {STG}.STG_DEPTH_LEVELS
            where symbol = ? and ingest_time >= dateadd('minute', -120, sysdate())
            qualify last_update_id = max(last_update_id) over (partition by symbol)
        )
        select side, level, price::float as price, qty::float as qty
        from latest order by price
    """, params=[symbol]).to_pandas()


@st.cache_data(ttl=10)
def load_cross_perf(_session):
    return _session.sql(f"""
        with b as (
            select symbol, minute, close,
                   first_value(close) over (partition by symbol order by minute) as first_close
            from {MARTS}.VW_OHLCV_1MIN_LIVE
        )
        select symbol, minute, (close / nullif(first_close, 0) * 100)::float as perf_100
        from b order by minute
    """).to_pandas()


def current_symbol():
    return st.session_state.get("symbol") or "BTCUSDT"


# =========================== PAGES ===========================
def page_home():
    # Page d'accueil epuree : un titre + un visuel (graphe en aire, SVG inline).
    svg = (
        '<svg width="440" height="190" viewBox="0 0 440 190" xmlns="http://www.w3.org/2000/svg">'
        '<defs><linearGradient id="g" x1="0" x2="0" y1="0" y2="1">'
        '<stop offset="0%" stop-color="#1e3a8a" stop-opacity="0.35"/>'
        '<stop offset="100%" stop-color="#1e3a8a" stop-opacity="0"/></linearGradient></defs>'
        '<path d="M0,150 L62,128 L124,138 L186,96 L248,108 L310,58 L372,74 L430,30 '
        'L430,190 L0,190 Z" fill="url(#g)"/>'
        '<path d="M0,150 L62,128 L124,138 L186,96 L248,108 L310,58 L372,74 L430,30" '
        'fill="none" stroke="#1e3a8a" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>'
        '<circle cx="430" cy="30" r="5" fill="#1e3a8a"/></svg>'
    )
    st.markdown(
        f"""
        <div style="text-align:center; padding-top:3.2rem">
          <h1 style="font-size:3.2rem; margin:0">Crypto Real-Time</h1>
          <p style="color:#1e3a8a; font-size:1.25rem; margin:0.5rem 0 0; font-weight:600">
            Poste de surveillance du flux d'ordres crypto
          </p>
          <p style="color:#64748b; font-size:1rem; margin:0.45rem auto 0; max-width:560px">
            Order-flow (qui achete / qui vend), anomalies de volume (ML) et fraicheur des donnees
            &mdash; pour lire le marche et decider vite, sans terminal pro couteux.
          </p>
          <div style="margin-top:2rem">{svg}</div>
          <p style="color:#94a3b8; font-size:0.85rem; margin-top:1.6rem">
            Snowflake &middot; dbt &middot; Cortex ML &middot; aide a la decision (pas un conseil d'investissement)
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.fragment(run_every=10)
def page_trader():
    # Vue RETAIL : traduit le flux d'ordres en langage simple (pression, RSI, volume).
    sym = current_symbol()
    st.header(f"Vue trader - {sym}")
    st.caption(
        "Lecture simplifiee du flux d'ordres. Detail sur Microstructure / Prix & carnet. "
        "Contexte, pas un conseil."
    )
    ohlcv, cvd, movers = load_ohlcv(session, sym), load_cvd(session, sym), load_movers(session)
    row = movers[movers["SYMBOL"] == sym]
    close = ohlcv["CLOSE"].iloc[-1] if not ohlcv.empty else None
    chg5 = row["PRICE_CHANGE_PCT_5MIN"].iloc[0] if not row.empty else None
    rsi = row["RSI_14"].iloc[0] if not row.empty else None
    vol_anom = bool(row["IS_VOLUME_ANOMALY"].iloc[0]) if not row.empty else False
    recent_delta = float(cvd["DELTA"].tail(10).sum()) if not cvd.empty else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Prix", "-" if close is None else f"{close:.2f}",
              None if (chg5 is None or pd.isna(chg5)) else f"{chg5:+.2f}% / 5min")
    pression = "Acheteurs" if recent_delta > 0 else "Vendeurs" if recent_delta < 0 else "Equilibre"
    c2.metric("Pression (10 min)", pression)
    if rsi is None or pd.isna(rsi):
        rsi_txt, rsi_val = "warm-up", None
    elif rsi >= 70:
        rsi_txt, rsi_val = "Surachat", f"{rsi:.0f}"
    elif rsi <= 30:
        rsi_txt, rsi_val = "Survente", f"{rsi:.0f}"
    else:
        rsi_txt, rsi_val = "Neutre", f"{rsi:.0f}"
    c3.metric("RSI", rsi_txt, rsi_val)
    c4.metric("Volume", "ANORMAL" if vol_anom else "Normal")

    bits = []
    if recent_delta > 0:
        bits.append("les acheteurs poussent (CVD en hausse)")
    elif recent_delta < 0:
        bits.append("les vendeurs poussent (CVD en baisse)")
    if rsi_txt == "Surachat":
        bits.append("RSI en surachat (essoufflement haussier possible)")
    elif rsi_txt == "Survente":
        bits.append("RSI en survente (rebond possible)")
    if vol_anom:
        bits.append("volume anormal en cours (un gros acteur ?)")
    verdict = "Lecture rapide : " + ("; ".join(bits) if bits else "marche calme, rien de notable") + "."
    (st.success if recent_delta > 0 else st.error if recent_delta < 0 else st.info)(verdict)

    if not ohlcv.empty:
        up_down = alt.condition("datum.OPEN <= datum.CLOSE", alt.value(UP), alt.value(DOWN))
        tip = [_tmin(), alt.Tooltip("OPEN:Q", format=".2f"), alt.Tooltip("HIGH:Q", format=".2f"),
               alt.Tooltip("LOW:Q", format=".2f"), alt.Tooltip("CLOSE:Q", format=".2f")]
        base = alt.Chart(ohlcv).encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)), color=up_down)
        wick = base.mark_rule().encode(
            y=alt.Y("LOW:Q", scale=alt.Scale(zero=False), title="prix"), y2="HIGH:Q")
        body = base.mark_bar(size=6).encode(y="OPEN:Q", y2="CLOSE:Q", tooltip=tip)
        st.altair_chart(_style(wick + body, 320), theme=None, width="stretch")


@st.fragment(run_every=10)
def page_prix():
    sym = current_symbol()
    st.header(f"Prix & carnet - {sym}")

    ohlcv = load_ohlcv(session, sym)
    ob = load_orderbook(session, sym)
    metrics = load_metrics(session, sym)
    m = metrics.iloc[-1] if not metrics.empty else None
    r = ob.iloc[0] if not ob.empty else None
    close = ohlcv["CLOSE"].iloc[-1] if not ohlcv.empty else None

    # --- Bandeau KPI (haut, pleine largeur) : la lecture rapide d'abord ---
    def _v(x, fmt):
        return "-" if x is None or pd.isna(x) else format(x, fmt)

    k = st.columns(5)
    k[0].metric("Prix", _v(close, ".2f"),
                None if m is None or pd.isna(m["PRICE_CHANGE_PCT_5MIN"]) else f"{m['PRICE_CHANGE_PCT_5MIN']:+.2f}% / 5min")
    k[1].metric("RSI 14", "warm-up" if m is None or pd.isna(m["RSI_14"]) else f"{m['RSI_14']:.0f}")
    k[2].metric("Volatilite", _v(None if m is None else m["REALIZED_VOLATILITY"], ".4f"))
    k[3].metric("Spread (bps)", _v(None if r is None else r["SPREAD_BPS"], ".2f"))
    k[4].metric("Imbalance", "-" if r is None else f"{r['IMBALANCE']:.2%}")

    st.divider()

    # --- Bougies + volume (gauche) | carnet (droite) ---
    col1, col2 = st.columns([3, 1])
    with col1:
        if ohlcv.empty:
            st.info("Pas de donnees recentes pour ce symbole.")
        else:
            up_down = alt.condition("datum.OPEN <= datum.CLOSE", alt.value(UP), alt.value(DOWN))
            tip = [_tmin(), alt.Tooltip("OPEN:Q", format=".2f"), alt.Tooltip("HIGH:Q", format=".2f"),
                   alt.Tooltip("LOW:Q", format=".2f"), alt.Tooltip("CLOSE:Q", format=".2f"),
                   alt.Tooltip("VOLUME:Q", format=".2f")]
            base = alt.Chart(ohlcv).encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)), color=up_down)
            wick = base.mark_rule().encode(
                y=alt.Y("LOW:Q", scale=alt.Scale(zero=False), title="prix"), y2="HIGH:Q")
            body = base.mark_bar(size=6).encode(y="OPEN:Q", y2="CLOSE:Q", tooltip=tip)
            st.altair_chart(_style(wick + body, 340), theme=None, width="stretch")
            vol = alt.Chart(ohlcv).mark_bar().encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                y=alt.Y("VOLUME:Q", title="volume"), color=up_down, tooltip=tip)
            st.altair_chart(_style(vol, 130), theme=None, width="stretch")
    with col2:
        st.subheader("Carnet (live)")
        if r is None:
            st.info("Carnet indisponible.")
        else:
            st.metric("Mid", f"{r['MID']:.2f}")
            st.metric("Microprice", f"{r['MICROPRICE']:.2f}")
            st.caption(f"Spread {r['SPREAD_BPS']:.2f} bps · imbalance {r['IMBALANCE']:.0%}")

    if not metrics.empty and metrics["RSI_14"].notna().any():
        st.subheader("RSI 14 (Wilder)")
        rsi_line = alt.Chart(metrics).mark_line(interpolate="monotone", color=ACCENT, strokeWidth=2).encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("RSI_14:Q", scale=alt.Scale(domain=[0, 100]), title="RSI 14"),
            tooltip=[_tmin(), alt.Tooltip("RSI_14:Q", format=".1f")])
        b70 = alt.Chart(metrics).mark_rule(strokeDash=[4, 4], color=REF).encode(y=alt.datum(70))
        b30 = alt.Chart(metrics).mark_rule(strokeDash=[4, 4], color=REF).encode(y=alt.datum(30))
        st.altair_chart(_style(rsi_line + b70 + b30, 180), theme=None, width="stretch")

    st.subheader("Top movers (5 min)")
    st.dataframe(load_movers(session), width="stretch")


@st.fragment(run_every=10)
def page_micro():
    sym = current_symbol()
    st.header(f"Microstructure - {sym}")

    st.subheader("Flux taker - CVD (Cumulative Volume Delta)")
    st.caption("CVD > 0 = pression acheteuse nette depuis le debut de la fenetre.")
    cvd = load_cvd(session, sym)
    if cvd.empty:
        st.info("Pas de trades recents.")
    else:
        cvd_area = alt.Chart(cvd).mark_area(
            interpolate="monotone", line={"color": ACCENT, "strokeWidth": 2}, opacity=0.5,
            color=alt.Gradient(gradient="linear", x1=1, x2=1, y1=1, y2=0, stops=[
                alt.GradientStop(color="white", offset=0), alt.GradientStop(color=ACCENT, offset=1)]),
        ).encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("CVD:Q", title="CVD (cumul buy - sell)"),
            tooltip=[_tmin(), alt.Tooltip("CVD:Q", format=".2f")])
        zero = alt.Chart(cvd).mark_rule(strokeDash=[4, 4], color=REF).encode(y=alt.datum(0))
        st.altair_chart(_style(cvd_area + zero, 220), theme=None, width="stretch")
        delta_bars = alt.Chart(cvd).mark_bar().encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("DELTA:Q", title="delta / min"),
            color=alt.condition("datum.DELTA >= 0", alt.value(UP), alt.value(DOWN)),
            tooltip=[_tmin(), alt.Tooltip("DELTA:Q", format=".2f")])
        st.altair_chart(_style(delta_bars, 120), theme=None, width="stretch")

    st.subheader("Carnet d'ordres - ladder (dernier snapshot)")
    levels = load_orderbook_levels(session, sym)
    if levels.empty:
        st.info("Pas de snapshot de carnet recent.")
    else:
        ladder = alt.Chart(levels).mark_bar().encode(
            y=alt.Y("PRICE:O", sort="descending", title="prix"),
            x=alt.X("QTY:Q", title="quantite"),
            color=alt.Color("SIDE:N", title=None,
                            scale=alt.Scale(domain=["bid", "ask"], range=[UP, DOWN])),
            tooltip=[alt.Tooltip("SIDE:N"), alt.Tooltip("PRICE:Q", format=".2f"),
                     alt.Tooltip("QTY:Q", format=".4f")])
        st.altair_chart(_style(ladder, 440), theme=None, width="stretch")


@st.fragment(run_every=10)
def page_cross():
    st.header("Cross-symbole")
    xperf = load_cross_perf(session)
    if xperf.empty:
        st.info("Pas de donnees.")
        return
    st.subheader("Performance normalisee (base 100)")
    lines = alt.Chart(xperf).mark_line(interpolate="monotone", strokeWidth=2).encode(
        x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
        y=alt.Y("PERF_100:Q", scale=alt.Scale(zero=False), title="base 100"),
        color=alt.Color("SYMBOL:N", title=None),
        tooltip=[alt.Tooltip("SYMBOL:N"), _tmin(), alt.Tooltip("PERF_100:Q", format=".1f")])
    st.altair_chart(_style(lines, 280), theme=None, width="stretch")

    piv = xperf.pivot(index="MINUTE", columns="SYMBOL", values="PERF_100")
    corr = np.log(piv / piv.shift(1)).corr().round(2)
    if not corr.empty:
        st.subheader("Correlation des log-returns")
        st.dataframe(corr, width="stretch")


@st.fragment(run_every=10)
def page_surveillance():
    sym = current_symbol()
    st.header(f"Surveillance - anomalies de volume (Cortex ML) - {sym}")
    try:
        anom = load_anomalies(session, sym)
        if anom.empty:
            st.info("Aucune donnee scoree (couche ML non deployee ou pas de scoring recent).")
            return
        flagged = anom[anom["IS_ANOMALY"]]
        st.metric("Anomalies (2 h)", len(flagged))
        band = alt.Chart(anom).mark_area(opacity=0.12, color=ACCENT).encode(
            x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
            y=alt.Y("LOWER_BOUND:Q", title="volume"), y2="UPPER_BOUND:Q")
        line = alt.Chart(anom).mark_line(interpolate="monotone", color=ACCENT, strokeWidth=2).encode(
            x="MINUTE:T", y=alt.Y("VOLUME:Q", scale=alt.Scale(zero=False)),
            tooltip=[_tmin(), alt.Tooltip("VOLUME:Q", format=".2f")])
        pts = alt.Chart(flagged).mark_point(color=DOWN, size=80, filled=True).encode(
            x="MINUTE:T", y="VOLUME:Q",
            tooltip=[_tmin(), alt.Tooltip("VOLUME:Q", format=".2f")])
        st.altair_chart(_style(band + line + pts, 300), theme=None, width="stretch")
        st.caption("Bande = plage attendue ; points rouges = volume hors norme (modele entraine).")
    except Exception as exc:
        logger.warning("panneau anomalies indisponible: %s", exc)
        st.info("Couche surveillance non deployee (snowflake/07_ml_anomaly.sql).")


@st.fragment(run_every=10)
def page_sante():
    st.header("Sante du pipeline")

    try:
        slo = load_slo(session)
    except Exception as exc:
        logger.exception("panneau SLO indisponible")
        st.warning(f"SLO indisponible: {exc}")
        slo = None

    if slo is not None and not slo.empty:
        s = slo.iloc[0]
        fresh = None if pd.isna(s["FRESHNESS_S"]) else int(s["FRESHNESS_S"])

        # Banniere de statut global (lecture immediate)
        if fresh is None:
            st.info("Aucune donnee recente a evaluer.")
        elif fresh <= 120:
            st.success(f"Pipeline en bonne sante — donnees fraiches ({fresh} s).")
        else:
            st.error(f"Donnees obsoletes ({fresh} s > 120 s) — le consumer tourne-t-il ?")

        # KPI (avec la cible SLO en infobulle)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latence moy", "-" if pd.isna(s["AVG_S"]) else f"{s['AVG_S']:.2f} s")
        c2.metric("Latence p95", "-" if pd.isna(s["P95_S"]) else f"{s['P95_S']:.2f} s", help="SLO < 15 s")
        c3.metric("Debit", "-" if pd.isna(s["TRADES_PER_S"]) else f"{s['TRADES_PER_S']:.0f} /s")
        c4.metric("Fraicheur", "-" if fresh is None else f"{fresh} s", help="SLO <= 120 s")

        # Jauge : % du budget SLO consomme (vert sous la cible, rouge au-dela)
        rows = []
        if fresh is not None:
            rows.append({"SLO": "Fraicheur (/ 120 s)", "pct": fresh / 120 * 100})
        if not pd.isna(s["P95_S"]):
            rows.append({"SLO": "Latence p95 (/ 15 s)", "pct": s["P95_S"] / 15 * 100})
        if rows:
            budget = pd.DataFrame(rows)
            bar = alt.Chart(budget).mark_bar(cornerRadius=3, height=20).encode(
                x=alt.X("pct:Q", title="% du budget SLO consomme", scale=alt.Scale(domain=[0, 120])),
                y=alt.Y("SLO:N", title=None, sort="-x"),
                color=alt.condition("datum.pct <= 100", alt.value(UP), alt.value(DOWN)),
                tooltip=[alt.Tooltip("SLO:N", title="SLO"), alt.Tooltip("pct:Q", format=".0f", title="% budget")])
            rule = alt.Chart(pd.DataFrame({"x": [100]})).mark_rule(
                strokeDash=[5, 4], color=REF).encode(x="x:Q")
            st.altair_chart(_style(bar + rule, 130), theme=None, width="stretch")

    # Journal d'evenements (pipeline_log)
    try:
        events = load_events(session)
        if not events.empty:
            st.subheader("Derniers evenements")
            st.caption("Alertes fraicheur / anomalies / echecs de tests")
            st.dataframe(events, width="stretch", hide_index=True)
    except Exception as exc:
        logger.warning("journal pipeline_log indisponible: %s", exc)


# =========================== NAVIGATION ===========================
nav = st.navigation([
    st.Page(page_home, title="Accueil", icon="🏠", default=True),
    st.Page(page_trader, title="Vue trader", icon="🎯"),
    st.Page(page_prix, title="Prix & carnet", icon="📈"),
    st.Page(page_micro, title="Microstructure", icon="🌊"),
    st.Page(page_cross, title="Cross-symbole", icon="🔀"),
    st.Page(page_surveillance, title="Surveillance", icon="🚨"),
    st.Page(page_sante, title="Sante pipeline", icon="❤️"),
], position="sidebar")

# Filtre symbole partage (sidebar) -> applique a toutes les pages via session_state
with st.sidebar:
    st.markdown("### Filtre")
    _symbols = load_symbols(session)
    st.selectbox("Symbole", _symbols or ["BTCUSDT"], key="symbol")
    st.caption("Rafraichissement auto : 10 s")
    _utc = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        _loc = _utc.astimezone(ZoneInfo("Europe/Paris"))
        st.caption(f"Heures des graphes en UTC ({_utc:%H:%M} UTC = {_loc:%H:%M} Paris)")
    except Exception:
        st.caption("Heures des graphes en UTC (Paris = UTC+2 en ete, +1 en hiver)")

nav.run()
