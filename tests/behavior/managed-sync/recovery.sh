#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail
# shellcheck source=tests/behavior/managed-sync/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

printf '%s\n' 'recovery: establish a published tree'
establish_pair
mkdir -p "$replica_a/tools"
printf '%s\n' '#!/bin/sh' 'printf "managed sync executable\\n"' >"$replica_a/tools/run.sh"
chmod u+x "$replica_a/tools/run.sh"
sync_a >/dev/null
sync_b >/dev/null

printf '%s\n' 'recovery: rebuild a cold client under another local alias'
OFS_CONFIG="$cold_config" "$OFS_BIN" volume create "$cold_alias" \
  "${volume_options[@]}" >/dev/null
sync_cold >/dev/null
diff -ru "$replica_a" "$cold_replica" || fail 'cold replica does not match the published tree'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$cold_replica/tools/run.sh" ]] || fail 'cold rebuild lost executable bit'
fi

before_noop=$(tree_digest "$cold_replica")
sync_cold >/dev/null
[[ "$(tree_digest "$cold_replica")" == "$before_noop" ]] || \
  fail 'a no-op sync changed user-visible files'

printf '%s\n' 'published from the recovered client' >"$cold_replica/recovered-client.txt"
sync_cold >/dev/null
sync_a >/dev/null
diff -ru "$replica_a" "$cold_replica" || \
  fail 'the original client did not converge after the recovered client published'

printf '%s\n' 'recovery: status exposes durable replica state without secrets'
status_json=$(OFS_CONFIG="$cold_config" "$OFS_BIN" status --state "$cold_state" --json)
grep -Eq '"volume_model"[[:space:]]*:[[:space:]]*"managed"' <<<"$status_json" || \
  fail 'status did not report volume_model=managed'
grep -Eq '"access_model"[[:space:]]*:[[:space:]]*"sync"' <<<"$status_json" || \
  fail 'status did not report access_model=sync'
grep -Eq '"stable_rename_identity"[[:space:]]*:[[:space:]]*true' <<<"$status_json" || \
  fail 'status did not report the admitted native rename capability'
grep -Eq '"volume_alias"[[:space:]]*:[[:space:]]*"recovered-workspace"' <<<"$status_json" || \
  fail 'status did not report the current client local alias'
grep -Eq '"volume_id"[[:space:]]*:[[:space:]]*"[0-9a-f]{32}"' <<<"$status_json" || \
  fail 'status did not report the durable remote volume identity'
common_sequence=$(json_field "$status_json" common_sequence)
[[ "$common_sequence" =~ ^[1-9][0-9]*$ ]] || \
  fail 'status did not report a durable non-genesis common sequence'
original_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_a" --json)
[[ "$(json_field "$original_status" common_sequence)" == "$common_sequence" ]] || \
  fail 'converged replicas reported different common sequences'
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*0' <<<"$status_json" || \
  fail 'status reports unresolved conflicts'

for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN OFS_D1_TOKEN; do
  secret=${!name:-}
  if [[ -n "$secret" ]] && grep -Fq -- "$secret" <<<"$status_json"; then
    fail "status leaked credential from $name"
  fi
done
while IFS= read -r secret; do
  if [[ -n "$secret" ]] && grep -Fq -- "$secret" <<<"$status_json"; then
    fail 'status leaked a value listed in OFS_SECRET_PROBES'
  fi
done <<<"${OFS_SECRET_PROBES:-}"

printf 'managed-sync recovery passed (%s metadata)\n' "$OFS_METADATA_MODE"
