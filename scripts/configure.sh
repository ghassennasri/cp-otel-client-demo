#!/usr/bin/env bash
# Create .env from .env.example on the first run, with a random Grafana password.
# An existing .env is kept as is (only a missing password is added).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  umask 077
  cp .env.example .env
fi
if ! grep -Eq '^GRAFANA_PASSWORD=.+' .env; then
  password="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
  grep -v '^GRAFANA_PASSWORD=' .env > .env.tmp || true
  printf 'GRAFANA_PASSWORD=%s\n' "$password" >> .env.tmp
  mv .env.tmp .env
  chmod 600 .env
fi
printf '%s\n' 'Settings in .env (DEMO_ENV, DEMO_CLUSTER_NAME). Grafana user: demo, password in .env.'
