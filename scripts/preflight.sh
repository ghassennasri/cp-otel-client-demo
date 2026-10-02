#!/usr/bin/env bash
# Check the host before deploying: Linux amd64, rootful Docker with cgroup v2,
# enough memory, and vm.max_map_count for OpenSearch.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v docker >/dev/null
command -v python3 >/dev/null
docker compose version
docker info >/dev/null
if [[ "$(uname -s)" != Linux ]]; then
  echo 'Use a dedicated Linux VM with Docker Engine and cgroup v2; see README.' >&2
  exit 1
fi
[[ "$(docker info --format '{{.Architecture}}')" =~ ^(x86_64|amd64)$ ]] || { echo 'This demo is qualified for Linux amd64 only.' >&2; exit 1; }
[[ "$(docker info --format '{{.CgroupVersion}}')" == 2 ]] || { echo 'cgroup v2 required.' >&2; exit 1; }
[[ "$(sysctl -n vm.max_map_count)" -ge 262144 ]] || { echo 'Set on the dedicated VM: sudo sysctl -w vm.max_map_count=262144' >&2; exit 1; }
python3 - <<'PYCODE'
import json,subprocess
x=json.loads(subprocess.check_output(['docker','info','--format','{{json .}}']))
if any('rootless' in o for o in x.get('SecurityOptions',[])): raise SystemExit('Use a rootful Docker Engine on the isolated lab VM.')
if x['MemTotal'] < 24*1024**3: raise SystemExit('Allocate at least 24 GiB RAM to Docker; 32 GiB recommended.')
PYCODE
docker compose config --quiet
