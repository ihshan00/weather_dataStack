#!/bin/bash
set -euo pipefail

# ---------- Config ----------
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60}"   # seconds to wait per host
RUN_DB_MIGRATIONS="${RUN_DB_MIGRATIONS:-true}"
CREATE_ADMIN_USER="${CREATE_ADMIN_USER:-false}"
AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
PATH="$HOME/.local/bin:$PATH"  # Ensure pip-installed airflow is in PATH

log() { printf '%s %s\n' "[$(date --iso-8601=seconds)]" "$*"; }

# ---------- Functions ----------
wait_for_host() {
  local host="$1"; local port="$2"; local timeout="$3"
  local i=0
  log "Waiting for ${host}:${port} (timeout ${timeout}s)..."
  while [ "$i" -lt "$timeout" ]; do
    if bash -c "cat < /dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      log "OK: ${host}:${port} reachable."
      return 0
    fi
    i=$((i+1))
    sleep 1
  done
  log "ERROR: Timed out waiting for ${host}:${port} after ${timeout}s"
  return 1
}

install_requirements_if_present() {
  if [ -f "$AIRFLOW_HOME/requirements.txt" ]; then
    log "Found requirements.txt - installing..."
    pip install --no-cache-dir -r "$AIRFLOW_HOME/requirements.txt" || {
      log "Warning: pip install failed. Check logs."
    }
  else
    log "No requirements.txt found — skipping."
  fi
}

run_db_migrations() {
  log "Running Airflow DB upgrade..."
  airflow db upgrade
  log "Airflow DB upgrade finished."
}

create_admin_user() {
  log "Creating Airflow admin user..."
  set +e
  airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}"
  rc=$?
  set -e
  if [ $rc -eq 0 ]; then
    log "Admin user created."
  else
    log "Admin creation returned $rc (may already exist). Continuing."
  fi
}

# ---------- Main ----------
if [ $# -lt 1 ]; then
  echo "Usage: $0 <airflow-subcommand> [args...]"
  exit 1
fi

# Wait for Postgres
wait_for_host "$POSTGRES_HOST" "$POSTGRES_PORT" "$WAIT_TIMEOUT"

# Install python deps if requirements present
install_requirements_if_present

# Initialize DB if needed
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    log "Initializing Airflow DB..."
    airflow db init
else
    log "Airflow DB already initialized."
fi

# Run migrations if requested
if [ "$RUN_DB_MIGRATIONS" = "true" ] || [ "$RUN_DB_MIGRATIONS" = "1" ]; then
    run_db_migrations
fi

create_admin_user

# Start Airflow service passed via docker-compose command
log "Starting Airflow command: airflow $*"
exec airflow "$@"
