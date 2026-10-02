#!/usr/bin/env bash
# Checks that need Docker but no running lab: Compose file, gateway and agent configs
# (validated by the real otelcol-contrib binary), Ansible syntax, unit tests.
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/configure.sh
docker compose config --quiet
docker compose build ansible
mkdir -p artifacts
docker compose run --rm --no-deps otel-gateway validate --config=/etc/otelcol/config.yaml
docker compose run --rm ansible ansible/deploy-cp.yml --syntax-check
docker compose run --rm ansible ansible/deploy-otel.yml --syntax-check
# Renders one agent config per inventory host into artifacts/rendered/.
docker compose run --rm --entrypoint python3 -v "$PWD/artifacts:/demo/artifacts" ansible tests/check-static.py
for config in artifacts/rendered/*.yaml; do
  docker compose run --rm --no-deps -v "$PWD/$config:/tmp/agent.yaml:ro" otel-gateway validate --config=/tmp/agent.yaml
done
docker compose run --rm --entrypoint python3 ansible -m unittest discover -s tests -p "test_*.py" -v
