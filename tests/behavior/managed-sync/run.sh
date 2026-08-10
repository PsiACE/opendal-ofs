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
ofs_access_key=ofs-managed-sync
ofs_secret_key=ofs-managed-sync-password
binary="$workspace/target/debug/ofs"
fixtures_started=false
audit_root=
declare -a compose

usage() {
  cat <<'EOF'
Usage: tests/behavior/managed-sync/run.sh <COMMAND>

Commands:
  test all
  test <admission|smoke|reconcile|recovery|scale> <object|d1>
  test branch <object|d1>
  test staging
  perf [--baseline REF_OR_BINARY] [--candidate REF_OR_BINARY]
       [--rounds N] [OUTPUT]
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
    OFS_MANAGED_SYNC_AUDIT_DIR="$audit_root" \
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
  local with_d1=$1
  local -a services=(minio)
  mkdir -p "$audit_root"
  [[ $with_d1 == true ]] && services+=(d1)
  compose_run up --detach "${services[@]}" >/dev/null
  wait_for_http "http://127.0.0.1:$minio_port/minio/health/ready"
  if [[ $with_d1 == true ]]; then
    wait_for_http "http://127.0.0.1:$d1_port/health"
  fi
  compose_run run --rm -T minio-client \
    "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null; \
     mc mb --ignore-existing local/managed-sync >/dev/null; \
     mc admin user info local $ofs_access_key >/dev/null 2>&1 || \
       mc admin user add local $ofs_access_key $ofs_secret_key >/dev/null; \
     mc admin policy attach local readwrite --user $ofs_access_key >/dev/null" \
    >/dev/null
  printf 'Managed Sync fixtures are ready: MinIO http://127.0.0.1:%s' "$minio_port"
  [[ $with_d1 == true ]] && printf ', D1 http://127.0.0.1:%s/client/v4' "$d1_port"
  printf '.\n'
}

fixtures_down() {
  compose_run down --volumes --remove-orphans >/dev/null 2>&1
}

cleanup() {
  local status=$?
  trap - EXIT
  if [[ $fixtures_started == true ]] && ! fixtures_down; then
    ((status == 0)) && status=1
  fi
  if [[ -n $audit_root ]]; then
    rm -rf -- "$audit_root"
  fi
  exit "$status"
}

case_environment() {
  local root=$1 storage=$2 metadata=$3
  export OFS_BIN="$binary" OFS_CASE_ROOT="$root" OFS_STORAGE_URL="$storage"
  export OFS_METADATA_MODE="$metadata"
  export AWS_ACCESS_KEY_ID="$ofs_access_key" AWS_SECRET_ACCESS_KEY="$ofs_secret_key"
  export AWS_REGION=us-east-1
  unset OFS_METADATA_URL OFS_D1_TOKEN OFS_AUDIT_LOG
  if [[ $metadata == d1 ]]; then
    local case_id api_base
    case_id=$(basename "$(dirname "$root")")
    api_base="http%3A%2F%2F127.0.0.1%3A${d1_port}%2Fclient%2Fv4"
    export OFS_METADATA_URL="d1://local/managed-sync/${case_id}?api_base=${api_base}"
    export OFS_D1_TOKEN=local-d1-token
  fi
}

run_case() {
  local suite=$1 metadata=$2 run_root case_id endpoint storage script case_root d1_before=0
  run_root=$(mktemp -d "${TMPDIR:-/tmp}/ofs-managed-${suite}.XXXXXX")
  case_id=$(basename "$run_root")
  case_root="$run_root/case"
  case $suite in
    admission|smoke|reconcile|recovery|scale)
      script="$workspace/tests/behavior/managed-sync/${suite}.sh"
      ;;
    branch) script="$workspace/tests/behavior/managed-branch/workflow.sh" ;;
    staging)
      script="$workspace/tests/behavior/managed-sync/staging.sh"
      case_root=$run_root
      ;;
    *) fail "unknown acceptance suite: $suite" ;;
  esac
  endpoint="http%3A%2F%2F127.0.0.1%3A${minio_port}"
  storage="s3://managed-sync/${case_id}?endpoint=${endpoint}&region=us-east-1"
  case_environment "$case_root" "$storage" "$metadata"
  if [[ $metadata == d1 ]]; then
    d1_before=$(wc -l <"$audit_root/d1.jsonl")
  fi
  if bash "$script"; then
    if [[ $metadata == d1 ]]; then
      python3 - "$audit_root/d1.jsonl" "$d1_before" <<'PY'
import json
import pathlib
import sys

events = [json.loads(line) for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
events = events[int(sys.argv[2]):]
print(
    "D1 native HTTP audit: "
    f"requests={len(events)} "
    f"request_bytes={sum(event['request_bytes'] for event in events)} "
    f"response_bytes={sum(event['response_bytes'] for event in events)} "
    f"statements={sum(event['statements'] for event in events)}"
)
PY
    fi
    rm -rf -- "$run_root"
  else
    printf 'Managed %s evidence retained at %s\n' "$suite" "$run_root" >&2
    return 1
  fi
}

run_tests() {
  local kind=${1:-}
  shift || true
  local -a cases
  case $kind in
    all)
      cases=(
        admission:object smoke:object reconcile:object recovery:object scale:object
        admission:d1 smoke:d1 reconcile:d1 recovery:d1 scale:d1
        branch:object branch:d1 staging:object
      )
      ;;
    admission|smoke|reconcile|recovery|scale|branch)
      [[ $# == 1 && $1 =~ ^(object|d1)$ ]] || fail "$kind requires object or d1 metadata"
      cases=("$kind:$1")
      ;;
    staging)
      [[ $# == 0 ]] || fail 'test staging accepts no arguments'
      cases=(staging:object)
      ;;
    *)
      fail 'expected test all, test admission|smoke|reconcile|recovery|scale|branch object|d1, or test staging'
      ;;
  esac
  command -v b3sum >/dev/null || fail 'b3sum is required'
  cargo build --locked
  select_compose
  audit_root=$(mktemp -d "${TMPDIR:-/tmp}/ofs-managed-audit.XXXXXX")
  fixtures_started=true
  trap cleanup EXIT
  local needs_d1=false item
  for item in "${cases[@]}"; do
    [[ ${item##*:} == d1 ]] && needs_d1=true
  done
  fixtures_up "$needs_d1"
  for item in "${cases[@]}"; do
    run_case "${item%%:*}" "${item##*:}"
  done
}

command=${1:--h}
shift || true
case $command in
  test) run_tests "$@" ;;
  perf) exec bash "$workspace/tests/performance/managed-sync/run.sh" "$@" ;;
  -h|--help) usage ;;
  *) fail "unknown command: $command" ;;
esac
