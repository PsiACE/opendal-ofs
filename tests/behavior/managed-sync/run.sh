#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$workspace"
compose_file="$workspace/fixtures/managed-sync/compose.yaml"
project=${OFS_MANAGED_SYNC_PROJECT:-opendal-ofs-managed-sync}
minio_port=${OFS_MANAGED_SYNC_MINIO_PORT:-19000}
d1_port=${OFS_MANAGED_SYNC_D1_PORT:-19001}
binary="$workspace/target/debug/ofs"
fixtures_started=false
proxy_pid=
proxy_port=
declare -a compose

usage() {
  cat <<'EOF'
Usage: cargo x managed-sync <COMMAND>

Commands:
  test all
  test workflow <object|d1>
  test branch <object|d1>
  test staging
  perf [--baseline REF_OR_BINARY] [--candidate REF_OR_BINARY]
       [--rounds N] [--profile standard|agent-home] [OUTPUT]
  up
  down
EOF
}

fail() { printf 'managed-sync harness: %s\n' "$*" >&2; exit 2; }

select_compose() {
  case ${OFS_COMPOSE:-} in
    docker) compose=(docker compose) ;;
    podman) compose=(podman compose) ;;
    podman-compose) compose=(podman-compose) ;;
    '')
      if docker compose version >/dev/null 2>&1; then
        compose=(docker compose)
      elif podman compose version >/dev/null 2>&1; then
        compose=(podman compose)
      elif podman-compose version >/dev/null 2>&1; then
        compose=(podman-compose)
      else
        fail 'Docker Compose or podman-compose is required'
      fi
      ;;
    *) fail 'OFS_COMPOSE must be docker, podman, or podman-compose' ;;
  esac
}

compose_run() {
  OFS_MANAGED_SYNC_MINIO_PORT="$minio_port" OFS_MANAGED_SYNC_D1_PORT="$d1_port" \
    "${compose[@]}" --project-name "$project" --file "$compose_file" "$@"
}

wait_for_http() {
  local url=$1
  for _ in $(seq 1 120); do
    curl --fail --silent --output /dev/null "$url" && return
    sleep 0.25
  done
  fail "fixture did not become ready: $url"
}

fixtures_up() {
  select_compose
  compose_run up --detach minio d1
  wait_for_http "http://127.0.0.1:$minio_port/minio/health/ready"
  wait_for_http "http://127.0.0.1:$d1_port/health"
  compose_run run --rm -T minio-client \
    'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null; mc mb --ignore-existing local/managed-sync >/dev/null; mc stat local/managed-sync >/dev/null'
  printf 'Managed Sync fixtures are ready: MinIO http://127.0.0.1:%s, D1 http://127.0.0.1:%s/client/v4.\n' \
    "$minio_port" "$d1_port"
}

fixtures_down() { select_compose; compose_run down --volumes --remove-orphans; }

cleanup() {
  local status=$?
  trap - EXIT
  stop_request_proxy
  if [[ $fixtures_started == true ]] && ! fixtures_down; then
    ((status == 0)) && status=1
  fi
  exit "$status"
}

start_request_proxy() {
  local root=$1 ready="$1/proxy.port"
  : >"$root/requests.jsonl"
  python3 "$workspace/tests/performance/managed-sync/s3-proxy.py" \
    --upstream "127.0.0.1:$minio_port" --log "$root/requests.jsonl" --ready "$ready" &
  proxy_pid=$!
  for _ in $(seq 1 100); do
    [[ -s $ready ]] && break
    kill -0 "$proxy_pid" 2>/dev/null || fail 'Managed Sync request proxy exited before ready'
    sleep 0.05
  done
  [[ -s $ready ]] || fail 'Managed Sync request proxy did not become ready'
  proxy_port=$(<"$ready")
}

stop_request_proxy() {
  if [[ -n $proxy_pid ]]; then
    kill "$proxy_pid" >/dev/null 2>&1 || true
    wait "$proxy_pid" 2>/dev/null || true
    proxy_pid=
    proxy_port=
  fi
}

case_environment() {
  local root=$1 storage=$2 metadata=$3
  export OFS_BIN="$binary" OFS_CASE_ROOT="$root" OFS_STORAGE_URL="$storage"
  export OFS_METADATA_MODE="$metadata"
  export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
  unset OFS_METADATA_URL OFS_D1_TOKEN OFS_REQUEST_LOG
  if [[ $metadata == d1 ]]; then
    local case_id api_base
    case_id=$(basename "$(dirname "$root")")
    api_base="http%3A%2F%2F127.0.0.1%3A${d1_port}%2Fclient%2Fv4"
    export OFS_METADATA_URL="d1://local/managed-sync/${case_id}?api_base=${api_base}"
    export OFS_D1_TOKEN=local-d1-token
  fi
}

run_case() {
  local suite=$1 metadata=$2 run_root case_id endpoint_port endpoint storage script case_root
  run_root=$(mktemp -d "${TMPDIR:-/tmp}/ofs-managed-${suite}.XXXXXX")
  case_id=$(basename "$run_root")
  case_root="$run_root/case"
  endpoint_port=$minio_port
  case $suite in
    workflow) script="$workspace/tests/behavior/managed-sync/workflow.sh" ;;
    branch) script="$workspace/tests/behavior/managed-branch/workflow.sh" ;;
    staging)
      script="$workspace/tests/performance/managed-sync/staging.sh"
      case_root=$run_root
      start_request_proxy "$run_root"
      endpoint_port=$proxy_port
      ;;
    *) fail "unknown acceptance suite: $suite" ;;
  esac
  endpoint="http%3A%2F%2F127.0.0.1%3A${endpoint_port}"
  storage="s3://managed-sync/${case_id}?endpoint=${endpoint}&region=us-east-1"
  case_environment "$case_root" "$storage" "$metadata"
  if [[ $suite == staging ]]; then
    export OFS_REQUEST_LOG="$run_root/requests.jsonl"
  fi
  if bash "$script"; then
    stop_request_proxy
    rm -rf -- "$run_root"
  else
    stop_request_proxy
    printf 'Managed %s evidence retained at %s\n' "$suite" "$run_root" >&2
    return 1
  fi
}

run_tests() {
  local kind=${1:-}
  shift || true
  local -a cases
  case $kind in
    all) cases=(workflow:object workflow:d1 branch:object branch:d1 staging:object) ;;
    workflow|branch)
      [[ $# == 1 && $1 =~ ^(object|d1)$ ]] || fail "$kind requires object or d1 metadata"
      cases=("$kind:$1")
      ;;
    staging)
      [[ $# == 0 ]] || fail 'test staging accepts no arguments'
      cases=(staging:object)
      ;;
    *) fail 'expected test all, test workflow|branch object|d1, or test staging' ;;
  esac
  cargo build --locked
  fixtures_started=true
  trap cleanup EXIT
  fixtures_up
  local item
  for item in "${cases[@]}"; do
    run_case "${item%%:*}" "${item##*:}"
  done
}

command=${1:--h}
shift || true
case $command in
  test) run_tests "$@" ;;
  perf) exec bash "$workspace/tests/performance/managed-sync/run.sh" "$@" ;;
  up) [[ $# == 0 ]] || fail 'up accepts no arguments'; fixtures_up ;;
  down) [[ $# == 0 ]] || fail 'down accepts no arguments'; fixtures_down ;;
  -h|--help) usage ;;
  *) fail "unknown command: $command" ;;
esac
