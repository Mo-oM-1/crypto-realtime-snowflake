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
CMD ["python", "stream_to_snowflake.py"]
