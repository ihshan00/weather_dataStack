#!/usr/bin/env bash
set -euo pipefail

export PATH="$PATH:/home/airflow/.local/bin"

if [ -f "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  python -m airflow db init
  python -m airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com --password admin || true
else
  python -m airflow db upgrade || true
fi

exec python -m airflow webserver
