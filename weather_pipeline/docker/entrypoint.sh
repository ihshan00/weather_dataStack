#!/usr/bin/env bash
set -euo pipefail

# Use the constraints file matching your Airflow/Python version
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.9.txt"

# If requirements.txt exists and is non-empty, install using constraints
if [ -s "/opt/airflow/requirements.txt" ]; then
  echo "Installing Python requirements..."
  python -m pip install --upgrade pip
  pip install --no-cache-dir -r /opt/airflow/requirements.txt --constraint "${CONSTRAINT_URL}"
fi

# locate airflow executable
AIRFLOW_CMD="$(command -v airflow || true)"
if [ -z "${AIRFLOW_CMD}" ]; then
  echo "ERROR: 'airflow' command not found in PATH. Exiting."
  exit 1
fi

# initialize DB if a DB file doesn't exist (sqlite dev) OR run upgrade for existing DB
if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo "Initializing Airflow DB..."
  "${AIRFLOW_CMD}" db init

  echo "Creating default admin user..."
  "${AIRFLOW_CMD}" users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || true
else
  echo "Airflow DB file detected; running db upgrade to apply migrations (if any)..."
  "${AIRFLOW_CMD}" db upgrade || true
fi

# start the webserver (exec so it receives signals)
echo "Starting webserver..."
exec "${AIRFLOW_CMD}" webserver
