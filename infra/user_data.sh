#!/usr/bin/env bash
# Bootstrap du consumer crypto - execute UNE fois au 1er boot par cloud-init.
# Provisionne : swap 2 Go, Python, repo, venv, service systemd.
#
# SECRETS : profile.json et rsa_key.p8 ne sont JAMAIS ici (ni dans l'IaC, ni au repo).
# Ils sont copies par scp apres l'apply (cf. infra/README.md), ou tires d'AWS Secrets
# Manager / SSM Parameter Store en vraie prod. Le service tourne en echec tant que les
# secrets manquent (Restart=always) puis demarre tout seul des qu'ils sont presents.
set -euxo pipefail

# --- Swap 2 Go (compense la RAM de 1 Go : pic du SDK Snowpipe au demarrage) ---
fallocate -l 2G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# --- Python + git ---
apt-get update -y
apt-get install -y python3-venv python3-pip git

# --- Repo + venv + dependances (sous l'utilisateur ubuntu) ---
sudo -u ubuntu bash <<'EOSU'
cd /home/ubuntu
git clone ${repo_url} crypto-realtime-snowflake
cd crypto-realtime-snowflake/ingestion
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
EOSU

# --- Service systemd (demarrage auto + relance sur crash) ---
cat >/etc/systemd/system/crypto-ingest.service <<'EOSVC'
[Unit]
Description=Crypto realtime consumer (Binance -> Snowpipe Streaming)
After=network-online.target
Wants=network-online.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/crypto-realtime-snowflake/ingestion
ExecStart=/home/ubuntu/crypto-realtime-snowflake/ingestion/venv/bin/python stream_to_snowflake.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOSVC

systemctl daemon-reload
systemctl enable --now crypto-ingest
