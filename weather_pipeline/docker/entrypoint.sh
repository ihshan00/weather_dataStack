#!/usr/bin/env bash
set -euo pipefail

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.9.txt"

if [ -s "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --no-cache-dir -r /opt/airflow/requirements.txt --constraint "${CONSTRAINT_URL}"
fi

# use python -m airflow to avoid PATH issues
if [ ! -f "/opt/airflow/airflow.db" ]; then
  python -m airflow db init
  python -m airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com --password admin || true
else
  python -m airflow db upgrade || true
fi

exec python -m airflow webserver
