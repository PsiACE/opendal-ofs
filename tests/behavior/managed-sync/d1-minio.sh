#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0.

set -euo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
runtime=${CONTAINER_RUNTIME:-podman}
actor=${1:-lifecycle}
ofs_bin=${OFS_BIN:-$workspace/target/debug/ofs}
: "${ACCOUNT_ID:?} ${D1_ID:?} ${D1_KEY:?}"
case $actor in
  lifecycle | scripted) ;;
  *)
    printf 'usage: %s [lifecycle|scripted]\n' "$0" >&2
    exit 2
    ;;
esac

run_root=$(mktemp -d)
container="ofs-managed-d1-${PPID}-$$"
store_key="ofs-${actor}-$(date +%s)-$$"

d1_execute() {
  local sql=$1 mode=${2:-execute} body response
  body=$(python3 - "$sql" "$store_key" <<'PY'
import json
import sys

sql = sys.argv[1]
store_key = sys.argv[2]
print(json.dumps({"sql": sql, "params": [store_key] * sql.count("?")}))
PY
)
  response=$(curl -fsS -X POST \
    "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/d1/database/${D1_ID}/query" \
    -H "Authorization: Bearer ${D1_KEY}" \
    -H 'Content-Type: application/json' \
    --data-binary "$body")
  python3 -c '
import json
import sys

response = json.load(sys.stdin)
results = response.get("result", [])
assert response.get("success") and len(results) == 1, response
query = results[0]
assert query.get("success"), query
assert query.get("meta", {}).get("served_by_primary") is True, query
if sys.argv[1] == "empty":
    assert query.get("results") == [{"retained": 0}], query
' "$mode" \
    <<<"$response"
}

cleanup() {
  status=$?
  for table in ofs_managed_heads ofs_managed_commits ofs_managed_checkpoints ofs_managed_formats; do
    if ! d1_execute "DELETE FROM ${table} WHERE store_key = ?"; then
      status=1
    fi
  done
  retained_sql='SELECT
    (SELECT COUNT(*) FROM ofs_managed_heads WHERE store_key = ?) +
    (SELECT COUNT(*) FROM ofs_managed_commits WHERE store_key = ?) +
    (SELECT COUNT(*) FROM ofs_managed_checkpoints WHERE store_key = ?) +
    (SELECT COUNT(*) FROM ofs_managed_formats WHERE store_key = ?)
    AS retained'
  if ! d1_execute "$retained_sql" empty; then
    status=1
  fi
  if ! "$runtime" rm -f "$container" >/dev/null 2>&1; then
    status=1
  fi
  if ((status == 0)); then
    rm -rf "$run_root"
  else
    printf 'Managed Sync D1 evidence retained at %s\n' "$run_root" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

test -x "$ofs_bin"
"$runtime" run -d --rm --name "$container" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=ofs-d1 \
  -e MINIO_ROOT_PASSWORD=ofs-d1-password \
  quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z server /data >/dev/null
port=$("$runtime" port "$container" 9000/tcp | sed -n 's/.*://p')
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:${port}/minio/health/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:${port}/minio/health/ready" >/dev/null

"$runtime" run --rm --network host --entrypoint /bin/sh \
  quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z -c \
  "mc alias set acceptance http://127.0.0.1:${port} ofs-d1 ofs-d1-password >/dev/null && mc mb acceptance/ofs-managed-sync >/dev/null"

export OFS_BIN="$ofs_bin"
export OFS_RUN_ROOT="$run_root"
export OFS_VOLUME=agent-home
export OFS_STORAGE_URL="s3://?bucket=ofs-managed-sync&root=${actor}&endpoint=http://127.0.0.1:${port}&region=us-east-1&access_key_id=ofs-d1&secret_access_key=ofs-d1-password"
export OFS_WRONG_STORAGE_URL="s3://?bucket=ofs-managed-sync&root=${actor}-wrong&endpoint=http://127.0.0.1:${port}&region=us-east-1&access_key_id=ofs-d1&secret_access_key=ofs-d1-password"
export OFS_PUBLIC_STORAGE_URL="s3://?bucket=ofs-managed-sync&root=${actor}&endpoint=http://127.0.0.1:${port}&region=us-east-1"
export OFS_METADATA_LOCATOR="d1://${ACCOUNT_ID}/${D1_ID}/${store_key}"
export OFS_METADATA_URL="${OFS_METADATA_LOCATOR}?token=${D1_KEY}"
export OFS_MINIO_ENDPOINT="http://127.0.0.1:${port}"
export OFS_CONTAINER_RUNTIME="$runtime"

case $actor in
  lifecycle) "$workspace/tests/behavior/managed-sync/lifecycle.sh" ;;
  scripted) "$workspace/tests/behavior/managed-sync/acceptance.sh" scripted ;;
esac
