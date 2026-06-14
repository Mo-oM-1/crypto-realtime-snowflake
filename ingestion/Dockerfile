FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stream_to_snowflake.py .

# Les secrets (profile.json, rsa_key.p8) sont MONTÉS au runtime, jamais copiés dans l'image :
#   docker run --rm \
#     -e SYMBOLS="btcusdt,ethusdt" \
#     -v "$PWD/profile.json:/app/profile.json:ro" \
#     -v "$PWD/rsa_key.p8:/app/rsa_key.p8:ro" \
#     crypto-ingest
ENV SS_LOG_LEVEL=warn
EXPOSE 8000

# Healthcheck : conteneur "unhealthy" si /healthz != 200 (consumer zombie :
# process vivant mais flux Binance mort). Avec `docker run --restart unless-stopped`,
# un conteneur unhealthy est relance automatiquement. start-period = fenetre de
# demarrage (1re connexion WebSocket) pour ne pas flagger un faux negatif au boot.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"

CMD ["python", "stream_to_snowflake.py"]
