"""
Dashboard Streamlit (in Snowflake) — crypto temps réel.

⚠️ À utiliser APRÈS que Cortex Code a généré les marts ($realtime-marts) :
    ANALYTICS.PUBLIC_MARTS.VW_OHLCV_1MIN_LIVE
    ANALYTICS.PUBLIC_MARTS.VW_ORDERBOOK_METRICS_LIVE
    ANALYTICS.PUBLIC_MARTS.VW_MARKET_METRICS_LIVE

Déploiement : Snowsight → Streamlit → New app (warehouse WH_CRYPTO_XS).
Les vues étant calculées à la lecture, chaque refresh affiche l'état ~temps réel.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(page_title="Crypto Real-Time", layout="wide")
st.title("📈 Crypto Real-Time — Snowflake + dbt")

MARTS = "ANALYTICS.PUBLIC_MARTS"

symbols = [r[0] for r in session.sql(
    f"SELECT DISTINCT symbol FROM {MARTS}.VW_OHLCV_1MIN_LIVE ORDER BY symbol"
).collect()]
symbol = st.selectbox("Symbole", symbols or ["BTCUSDT"])

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"OHLCV 1 min — {symbol}")
    ohlcv = session.sql(f"""
        SELECT minute, open, high, low, close, volume, vwap
        FROM {MARTS}.VW_OHLCV_1MIN_LIVE
        WHERE symbol = '{symbol}'
        ORDER BY minute
    """).to_pandas()
    if not ohlcv.empty:
        ohlcv = ohlcv.set_index("MINUTE")
        st.line_chart(ohlcv[["CLOSE", "VWAP"]])
        st.bar_chart(ohlcv[["VOLUME"]])

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
st.dataframe(movers, use_container_width=True)

st.caption("Vues calculées à la lecture → données fraîches à la seconde (Snowpipe Streaming).")
