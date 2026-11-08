#!/usr/bin/env bash
# Un solo comando: levanta los contenedores, prepara el venv y corre todo de punta a punta.
#   ./run.sh               (todo, ~15-25 min)
#   ./run.sh --sin-carga   (reutiliza el histórico ya cargado en el volumen)
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d --build --wait
if [ ! -x venv/bin/python ]; then python3 -m venv venv; fi
venv/bin/python -m pip install -q -r requirements.txt
PYTHONUTF8=1 venv/bin/python run_all.py "$@"
