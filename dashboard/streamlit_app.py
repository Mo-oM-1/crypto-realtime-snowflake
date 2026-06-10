"""
Dashboard Streamlit (in Snowflake) - crypto temps réel.

À utiliser APRÈS que Cortex Code a généré les marts ($realtime-marts) :
    ANALYTICS.PUBLIC_MARTS.VW_OHLCV_1MIN_LIVE
    ANALYTICS.PUBLIC_MARTS.VW_ORDERBOOK_METRICS_LIVE
    ANALYTICS.PUBLIC_MARTS.VW_MARKET_METRICS_LIVE

Déploiement : Snowsight -> Streamlit -> New app (warehouse WH_CRYPTO_XS).
Les vues étant calculées à la lecture, chaque refresh affiche l'état ~temps réel.
"""

import altair as alt
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(page_title="Crypto Real-Time", layout="wide")
st.title("Crypto Real-Time - Snowflake + dbt")

MARTS = "ANALYTICS.PUBLIC_MARTS"
MON = "ANALYTICS.MONITORING"

symbols = [r[0] for r in session.sql(
    f"SELECT DISTINCT symbol FROM {MARTS}.VW_OHLCV_1MIN_LIVE ORDER BY symbol"
).collect()]
symbol = st.selectbox("Symbole", symbols or ["BTCUSDT"])

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"OHLCV 1 min - {symbol}")
    ohlcv = session.sql(f"""
        SELECT minute, open, high, low, close, volume, vwap
        FROM {MARTS}.VW_OHLCV_1MIN_LIVE
        WHERE symbol = '{symbol}'
        ORDER BY minute
    """).to_pandas()
    if not ohlcv.empty:
        # Prix (close + vwap) : axe Y serré (zero=False) pour bien voir les 2 lignes,
        # axe X formaté en HH:MM (évite l'affichage ".500" au zoom).
        price = ohlcv.melt(
            id_vars="MINUTE", value_vars=["CLOSE", "VWAP"],
            var_name="série", value_name="prix",
        )
        line = (
            alt.Chart(price)
            .mark_line()
            .encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                y=alt.Y("prix:Q", scale=alt.Scale(zero=False), title="prix"),
                color=alt.Color("série:N", title=None),
            )
            .properties(height=300)
        )
        st.altair_chart(line, width="stretch")

        vol = (
            alt.Chart(ohlcv)
            .mark_bar()
            .encode(
                x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                y=alt.Y("VOLUME:Q", title="volume"),
            )
            .properties(height=160)
        )
        st.altair_chart(vol, width="stretch")

with col2:
    st.subheader("Order book (live)")
    ob = session.sql(f"""
        SELECT mid, spread_bps, imbalance, microprice
        FROM {MARTS}.VW_ORDERBOOK_METRICS_LIVE
        WHERE symbol = '{symbol}'
    """).to_pandas()
    if not ob.empty:
        r = ob.iloc[0]
        st.metric("Mid", f"{r['MID']:.2f}")
        st.metric("Spread (bps)", f"{r['SPREAD_BPS']:.2f}")
        st.metric("Imbalance", f"{r['IMBALANCE']:.2%}")
        st.metric("Microprice", f"{r['MICROPRICE']:.2f}")

st.subheader("Top movers (5 min)")
movers = session.sql(f"""
    SELECT symbol, price_change_pct_5min, is_volume_anomaly
    FROM {MARTS}.VW_MARKET_METRICS_LIVE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY minute DESC) = 1
    ORDER BY ABS(price_change_pct_5min) DESC
""").to_pandas()
st.dataframe(movers, width="stretch")

# --- Surveillance : anomalies de volume (Cortex ML) ----------------------
st.divider()
st.subheader("Surveillance - anomalies de volume (modèle Cortex ML)")
try:
    anom = session.sql(f"""
        SELECT symbol, minute, volume, forecast, lower_bound, upper_bound, is_anomaly, distance
        FROM {MON}.MART_VOLUME_ANOMALIES
        WHERE minute >= DATEADD('hour', -2, SYSDATE())
        ORDER BY minute
    """).to_pandas()
    if anom.empty:
        st.info("Aucune donnée scorée pour l'instant (lance snowflake/07_ml_anomaly.sql ou attends le prochain scoring).")
    else:
        flagged = anom[anom["IS_ANOMALY"]]
        c1, c2 = st.columns(2)
        c1.metric("Anomalies (2 h)", len(flagged))
        c2.metric("Points scorés (2 h)", len(anom))

        # Volume du symbole sélectionné + bande de confiance ; anomalies en rouge.
        a = anom[anom["SYMBOL"] == symbol]
        if not a.empty:
            band = (
                alt.Chart(a).mark_area(opacity=0.15)
                .encode(
                    x=alt.X("MINUTE:T", axis=alt.Axis(format="%H:%M", title=None)),
                    y=alt.Y("LOWER_BOUND:Q", title="volume"), y2="UPPER_BOUND:Q",
                )
            )
            line = (
                alt.Chart(a).mark_line()
                .encode(x="MINUTE:T", y=alt.Y("VOLUME:Q", scale=alt.Scale(zero=False)))
            )
            pts = (
                alt.Chart(a[a["IS_ANOMALY"]]).mark_point(color="red", size=90, filled=True)
                .encode(x="MINUTE:T", y="VOLUME:Q")
            )
            st.altair_chart((band + line + pts).properties(height=260), width="stretch")
        if not flagged.empty:
            st.dataframe(
                flagged[["SYMBOL", "MINUTE", "VOLUME", "UPPER_BOUND", "DISTANCE"]],
                width="stretch",
            )
except Exception:
    st.info("Couche surveillance non déployée (exécute snowflake/07_ml_anomaly.sql).")

# --- Santé du pipeline ---------------------------------------------------
st.divider()
st.subheader("Santé du pipeline")
try:
    fresh = session.sql(
        "SELECT DATEDIFF('second', MAX(ingest_time), SYSDATE()) AS s "
        "FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES"
    ).to_pandas()
    s = fresh.iloc[0]["S"]
    if s is not None:
        st.metric("Fraîcheur des trades (s)", int(s))
        if int(s) > 120:
            st.warning("Données obsolètes (> 120 s) - vérifie le consumer.")
    log = session.sql(f"""
        SELECT checked_at, metric, value, status
        FROM {MON}.pipeline_log
        ORDER BY checked_at DESC LIMIT 10
    """).to_pandas()
    if not log.empty:
        st.caption("Derniers événements (alertes fraîcheur / anomalies / tests)")
        st.dataframe(log, width="stretch")
except Exception:
    st.info("Schéma MONITORING non déployé (exécute snowflake/03_alerts.sql puis 07_ml_anomaly.sql).")

st.caption("Vues calculées à la lecture -> données fraîches à la seconde (Snowpipe Streaming).")
