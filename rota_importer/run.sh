#!/usr/bin/with-contenv bashio

export ROTA_DB_PATH=/config/rota.db

cd /app
uvicorn app:app --host 0.0.0.0 --port 8099