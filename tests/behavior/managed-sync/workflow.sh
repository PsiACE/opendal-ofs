#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

fail() {
  printf 'managed-sync acceptance: %s\n' "$*" >&2
  exit 1
}

tree_digest() {
  local root=$1
  (cd "$root" && find . -type f -exec sha256sum {} + | LC_ALL=C sort | sha256sum)
}

json_field() {
  local document=$1
  local field=$2
  python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$field" <<<"$document"
}

OFS_BIN=${OFS_BIN:-}
OFS_CASE_ROOT=${OFS_CASE_ROOT:-}
OFS_STORAGE_URL=${OFS_STORAGE_URL:-}
OFS_METADATA_MODE=${OFS_METADATA_MODE:-object}
OFS_METADATA_URL=${OFS_METADATA_URL:-}

[[ -n "$OFS_BIN" ]] || fail 'OFS_BIN must name the built ofs executable'
[[ -x "$OFS_BIN" ]] || fail "OFS_BIN is not executable: $OFS_BIN"
[[ -n "$OFS_CASE_ROOT" ]] || fail 'OFS_CASE_ROOT must name a fresh test directory'
[[ ! -e "$OFS_CASE_ROOT" ]] || fail "OFS_CASE_ROOT already exists: $OFS_CASE_ROOT"
[[ -n "$OFS_STORAGE_URL" ]] || fail 'OFS_STORAGE_URL must be a credential-free data URL'

case "$OFS_METADATA_MODE" in
  object)
    [[ -z "$OFS_METADATA_URL" ]] || fail 'object metadata uses OFS_STORAGE_URL; unset OFS_METADATA_URL'
    ;;
  d1)
    [[ -n "$OFS_METADATA_URL" ]] || fail 'd1 metadata requires OFS_METADATA_URL'
    ;;
  *)
    fail "OFS_METADATA_MODE must be object or d1, got: $OFS_METADATA_MODE"
    ;;
esac

config="$OFS_CASE_ROOT/client/config.json"
peer_config="$OFS_CASE_ROOT/peer-client/config.json"
cold_config="$OFS_CASE_ROOT/cold-client/config.json"
direct_config="$OFS_CASE_ROOT/direct-client/config.json"
extension_mismatch_config="$OFS_CASE_ROOT/extension-mismatch-client/config.json"
peer_alias=restored-workspace
cold_alias=recovered-workspace
replica_a="$OFS_CASE_ROOT/replica-a"
replica_b="$OFS_CASE_ROOT/replica-b"
cold_replica="$OFS_CASE_ROOT/cold-replica"
state_a="$OFS_CASE_ROOT/state/replica-a.json"
state_b="$OFS_CASE_ROOT/state/replica-b.json"
cold_state="$OFS_CASE_ROOT/state/cold-replica.json"

mkdir -p "$(dirname "$config")" "$(dirname "$peer_config")" \
  "$(dirname "$cold_config")" "$(dirname "$direct_config")" \
  "$(dirname "$extension_mismatch_config")" \
  "$replica_a" "$replica_b" "$cold_replica" "$(dirname "$state_a")"

printf '%s\n' 'acceptance: expose only named volume access commands'
cli_help=$("$OFS_BIN" --help)
grep -Eq '^  mount[[:space:]]' <<<"$cli_help" || fail 'help omitted the Mount access command'
grep -Eq '^  sync[[:space:]]' <<<"$cli_help" || fail 'help omitted the Sync access command'
grep -Eq '^  mount[[:space:]].*Direct.*read-only' <<<"$cli_help" || \
  fail 'help did not disclose the delivered Direct Mount boundary'
grep -Eq '^  sync[[:space:]].*Managed Sync' <<<"$cli_help" || \
  fail 'help did not disclose the delivered Managed Sync boundary'
if grep -Eq 'MOUNT_PATH.*BACKEND_URL|OFS_MOUNT_PATH|OFS_BACKEND' <<<"$cli_help"; then
  fail 'help still advertises the obsolete positional Direct Mount form'
fi
if "$OFS_BIN" volume create --help | grep -Fq -- '--transfer-concurrency'; then
  fail 'volume create still exposes an unused transfer concurrency option'
fi
direct_create=$(OFS_CONFIG="$direct_config" "$OFS_BIN" volume create archive \
  --model direct --storage 'memory:///acceptance')
grep -Fq 'registered direct volume alias "archive"' <<<"$direct_create" || \
  fail 'named Direct volume creation did not report its result'
direct_reopen=$(OFS_CONFIG="$direct_config" "$OFS_BIN" volume create archive \
  --model direct --storage 'memory:///acceptance')
grep -Fq 'verified direct volume alias "archive"' <<<"$direct_reopen" || \
  fail 'named Direct volume creation was not idempotent'
if direct_sync_error=$(OFS_CONFIG="$direct_config" "$OFS_BIN" sync archive "$cold_replica" \
  --state "$OFS_CASE_ROOT/state/direct.json" 2>&1); then
  fail 'Direct Sync started even though that access combination is unavailable'
fi
grep -Fq 'requires a Managed volume' <<<"$direct_sync_error" || \
  fail 'unavailable Direct Sync did not report an actionable admission error'

volume_options=(--model managed --storage "$OFS_STORAGE_URL")
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  volume_options+=(--metadata "$OFS_METADATA_URL")
fi

printf '%s\n' 'acceptance: register one managed volume under client-local aliases'
OFS_CONFIG="$config" "$OFS_BIN" volume create workspace "${volume_options[@]}"
OFS_CONFIG="$config" "$OFS_BIN" volume create workspace "${volume_options[@]}"
empty_gc=$(OFS_CONFIG="$config" "$OFS_BIN" volume gc workspace)
grep -Eq 'scanned=0 deleted=0 bytes=0' <<<"$empty_gc" || \
  fail 'garbage collection of an unpublished volume was not an empty success'
if extension_error=$(OFS_CONFIG="$extension_mismatch_config" "$OFS_BIN" volume create branching-workspace \
  "${volume_options[@]}" --enable branch 2>&1); then
  fail 'an explicit extension request changed an existing incompatible Managed volume'
fi
grep -Fq 'does not enable requested extension branch/v1' <<<"$extension_error" || \
  fail 'extension mismatch did not report the observed remote requirement'
[[ ! -e "$extension_mismatch_config" ]] || \
  fail 'extension mismatch wrote a local catalog binding'
OFS_CONFIG="$peer_config" "$OFS_BIN" volume create "$peer_alias" "${volume_options[@]}"
if duplicate_alias_error=$(OFS_CONFIG="$config" "$OFS_BIN" volume create duplicate-workspace \
  "${volume_options[@]}" 2>&1); then
  fail 'one catalog registered the same volume identity under two aliases'
fi
grep -Fq 'already registered as local alias "workspace"' <<<"$duplicate_alias_error" || \
  fail 'duplicate local binding did not report the existing alias'
if managed_mount_error=$(OFS_CONFIG="$config" "$OFS_BIN" mount workspace "$cold_replica" 2>&1); then
  fail 'Managed Mount started even though that access combination is unavailable'
fi
grep -Fq 'mount currently supports Direct volumes' <<<"$managed_mount_error" || \
  fail 'unavailable Managed Mount did not report an actionable admission error'

printf '%s\n' 'private before sync' >"$replica_a/first.txt"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/first.txt" ]] || fail 'volume creation published a local file without sync'

printf '%s\n' 'acceptance: first publication and empty-replica materialization'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
cmp "$replica_a/first.txt" "$replica_b/first.txt" || fail 'empty replica did not materialize first.txt'

printf '%s\n' 'acceptance: reject hard links before publication'
printf '%s\n' 'must remain local' >"$replica_a/hard-link-source.txt"
if ln "$replica_a/hard-link-source.txt" "$replica_a/hard-link-alias.txt" 2>/dev/null; then
  before_hard_link=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_a" --json)
  if hard_link_error=$(OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a" 2>&1); then
    fail 'hard-linked files were published'
  fi
  grep -Fq 'hard link' <<<"$hard_link_error" || fail 'hard-link rejection was not explicit'
  after_hard_link=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_a" --json)
  [[ "$before_hard_link" == "$after_hard_link" ]] || fail 'hard-link rejection changed replica state'
  rm "$replica_a/hard-link-alias.txt"
fi
rm "$replica_a/hard-link-source.txt"

printf '%s\n' 'acceptance: publish nested, empty, executable, and large files'
mkdir -p "$replica_a/nested/level" "$replica_a/tools"
printf '%s\n' 'created in a nested directory' >"$replica_a/nested/level/entry.txt"
printf '%s\n' 'removed after publication' >"$replica_a/nested/level/removed.txt"
: >"$replica_a/empty.bin"
dd if=/dev/zero of="$replica_a/large.bin" bs=1048576 count=8 2>/dev/null
printf '%s\n' '#!/bin/sh' 'printf "managed sync executable\\n"' >"$replica_a/tools/run.sh"
chmod u+x "$replica_a/tools/run.sh"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
printf '%s\n' 'acceptance: reuse known content at a new path'
cp "$replica_a/first.txt" "$replica_a/reused-content.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
cmp "$replica_a/reused-content.txt" "$replica_b/reused-content.txt" || \
  fail 'authority-known content did not round trip at a new path'
[[ -d "$replica_b/nested/level" ]] || fail 'nested directories were not materialized'
cmp "$replica_a/nested/level/entry.txt" "$replica_b/nested/level/entry.txt" || \
  fail 'nested file content did not round trip'
[[ ! -s "$replica_b/empty.bin" ]] || fail 'empty file did not round trip'
cmp "$replica_a/large.bin" "$replica_b/large.bin" || fail 'large file did not round trip'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$replica_b/tools/run.sh" ]] || fail 'executable bit did not round trip'
fi

printf '%s\n' 'acceptance: merge disjoint directory changes from two replicas'
mkdir -p "$replica_a/from-a/empty" "$replica_b/from-b/empty"
printf '%s\n' 'nested change from a' >"$replica_a/from-a/value.txt"
printf '%s\n' 'nested change from b' >"$replica_b/from-b/value.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
[[ -d "$replica_a/from-a/empty" && -d "$replica_a/from-b/empty" ]] || \
  fail 'replica a did not merge disjoint empty directories'
[[ -d "$replica_b/from-a/empty" && -d "$replica_b/from-b/empty" ]] || \
  fail 'replica b did not merge disjoint empty directories'
cmp "$replica_a/from-a/value.txt" "$replica_b/from-a/value.txt" || \
  fail 'replica b lost replica a nested directory change'
cmp "$replica_a/from-b/value.txt" "$replica_b/from-b/value.txt" || \
  fail 'replica a lost replica b nested directory change'

printf '%s\n' 'regression: reject a directory deletion that overlaps a local subtree change'
mkdir -p "$replica_a/overlap"
printf '%s\n' 'base' >"$replica_a/overlap/value.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
printf '%s\n' 'changed locally' >"$replica_a/overlap/value.txt"
rm -rf -- "$replica_b/overlap"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
overlap_tree=$(tree_digest "$replica_a")
overlap_state=$(sha256sum "$state_a")
if OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a" \
  2>"$OFS_CASE_ROOT/directory-overlap.err"; then
  fail 'overlapping remote directory deletion replaced a local subtree change'
fi
grep -Fq 'directory deletion overlaps local changes' \
  "$OFS_CASE_ROOT/directory-overlap.err" || fail 'directory overlap error was not actionable'
[[ "$(tree_digest "$replica_a")" == "$overlap_tree" ]] || \
  fail 'directory overlap rejection changed user files'
[[ "$(sha256sum "$state_a")" == "$overlap_state" ]] || \
  fail 'directory overlap rejection changed replica state'
rm -rf -- "$replica_a/overlap"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"

printf '%s\n' 'acceptance: modify, rename, and delete remote entries'
printf '%s\n' 'modified before rename' >"$replica_a/nested/level/entry.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
grep -Fxq 'modified before rename' "$replica_b/nested/level/entry.txt" || \
  fail 'remote file modification was not materialized'
mv "$replica_a/nested/level/entry.txt" "$replica_a/nested/renamed.txt"
rm "$replica_a/nested/level/removed.txt"
rmdir "$replica_a/nested/level"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/nested/level" ]] || fail 'deleted remote directory remained locally'
[[ ! -e "$replica_b/nested/level/entry.txt" ]] || fail 'old remote rename path remained locally'
[[ ! -e "$replica_b/nested/level/removed.txt" ]] || fail 'deleted remote file remained locally'
grep -Fxq 'modified before rename' "$replica_b/nested/renamed.txt" || \
  fail 'remote file rename was not materialized'

printf '%s\n' 'acceptance: preserve a moved directory tree'
mkdir -p "$replica_a/tree-before/branch/empty"
printf '%s\n' 'directory identity survives a move' >"$replica_a/tree-before/branch/leaf.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
mv "$replica_a/tree-before" "$replica_a/tree-after"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/tree-before" ]] || fail 'old directory move path remained remotely'
[[ -d "$replica_b/tree-after/branch/empty" ]] || fail 'moved empty directory was not materialized'
grep -Fxq 'directory identity survives a move' "$replica_b/tree-after/branch/leaf.txt" || \
  fail 'moved directory subtree content was not materialized'

printf '%s\n' 'published by replica a' >"$replica_a/a-only.txt"
printf '%s\n' 'published by replica b' >"$replica_b/b-only.txt"
printf '%s\n' 'acceptance: merge disjoint changes from two replicas'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
cmp "$replica_a/a-only.txt" "$replica_b/a-only.txt" || fail 'replica b lost replica a disjoint change'
cmp "$replica_a/b-only.txt" "$replica_b/b-only.txt" || fail 'replica a lost replica b disjoint change'

printf '%s\n' 'common base' >"$replica_a/shared.txt"
printf '%s\n' 'second common base' >"$replica_a/shared-two.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
printf '%s\n' 'candidate from replica a' >"$replica_a/shared.txt"
printf '%s\n' 'second candidate from replica a' >"$replica_a/shared-two.txt"
printf '%s\n' 'candidate from replica b' >"$replica_b/shared.txt"
printf '%s\n' 'second candidate from replica b' >"$replica_b/shared-two.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"

printf '%s\n' 'acceptance: retain and report same-path conflicts'
if OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"; then
  fail 'same-path concurrent edits succeeded without an explicit resolution'
fi
grep -Fxq 'candidate from replica a' "$replica_a/shared.txt" || fail 'remote conflict candidate was lost'
grep -Fxq 'candidate from replica b' "$replica_b/shared.txt" || fail 'local conflict candidate was lost'
grep -Fxq 'second candidate from replica a' "$replica_a/shared-two.txt" || \
  fail 'second remote conflict candidate was lost'
grep -Fxq 'second candidate from replica b' "$replica_b/shared-two.txt" || \
  fail 'second local conflict candidate was lost'
conflict_status=$(OFS_CONFIG="$peer_config" "$OFS_BIN" status --state "$state_b" --json)
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*2' <<<"$conflict_status" || \
  fail 'status did not report both unresolved conflicts'

printf '%s\n' 'acceptance: resolve multiple conflicts explicitly with retained local candidates'
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b" \
  --resolve shared.txt --resolve shared-two.txt
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
grep -Fxq 'candidate from replica b' "$replica_a/shared.txt" || fail 'resolved content was not published'
grep -Fxq 'second candidate from replica b' "$replica_a/shared-two.txt" || \
  fail 'second resolved content was not published'

printf '%s\n' 'acceptance: recover a durable publication intent after process death'
mkdir "$replica_a/crash-recovery"
for index in $(seq -w 1 128); do
  {
    printf 'crash recovery file %s\n' "$index"
    head -c 65536 /dev/zero
  } >"$replica_a/crash-recovery/$index.bin"
done
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a" &
crash_pid=$!
intent_observed=false
for _ in $(seq 1 200); do
  if ! kill -0 "$crash_pid" 2>/dev/null; then
    break
  fi
  recovery_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_a" --json 2>/dev/null || true)
  if grep -Eq '"pending"[[:space:]]*:[[:space:]]*true' <<<"$recovery_status"; then
    if kill -KILL "$crash_pid" 2>/dev/null; then
      intent_observed=true
    fi
    break
  fi
  sleep 0.01
done
wait "$crash_pid" 2>/dev/null || true
[[ "$intent_observed" == true ]] || fail 'could not interrupt sync after its intent became durable'
recovery_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_a" --json)
grep -Eq '"pending"[[:space:]]*:[[:space:]]*true' <<<"$recovery_status" || \
  fail 'process death lost the durable publication intent'
recovered=false
for _ in $(seq 1 5); do
  if OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"; then
    recovered=true
    break
  fi
  sleep 0.05
done
[[ "$recovered" == true ]] || fail 'repeated sync could not resolve the durable publication intent'
recovery_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_a" --json)
grep -Eq '"pending"[[:space:]]*:[[:space:]]*false' <<<"$recovery_status" || \
  fail 'recovered publication did not clear its completed intent'
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
cmp "$replica_a/crash-recovery/128.bin" "$replica_b/crash-recovery/128.bin" || \
  fail 'recovered publication did not materialize on another replica'

printf '%s\n' 'acceptance: recover through a long change history'
for generation in $(seq 1 60); do
  printf 'history change %s\n' "$generation" >"$replica_a/checkpoint.txt"
  OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a" >/dev/null
done
OFS_CONFIG="$peer_config" "$OFS_BIN" sync "$peer_alias" "$replica_b" --state "$state_b"
grep -Fxq 'history change 60' "$replica_b/checkpoint.txt" || \
  fail 'replica did not recover the fixed target after a long change history'
printf '%s\n' 'regression: collect obsolete segments without changing the live tree'
garbage_collection=$(OFS_CONFIG="$config" "$OFS_BIN" volume gc workspace)
grep -Eq 'deleted=[1-9][0-9]*' <<<"$garbage_collection" || \
  fail 'namespace-fenced collection removed no unreachable data segments'

printf '%s\n' 'acceptance: rebuild a cold client under another local alias'
OFS_CONFIG="$cold_config" "$OFS_BIN" volume create "$cold_alias" "${volume_options[@]}"
OFS_CONFIG="$cold_config" "$OFS_BIN" sync "$cold_alias" "$cold_replica" --state "$cold_state"
diff -ru "$replica_a" "$cold_replica" || fail 'cold replica does not match the published tree'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$cold_replica/tools/run.sh" ]] || fail 'cold rebuild lost executable bit'
fi

before_noop=$(cd "$cold_replica" && find . -type f -exec sha256sum {} + | LC_ALL=C sort)
OFS_CONFIG="$cold_config" "$OFS_BIN" sync "$cold_alias" "$cold_replica" --state "$cold_state"
after_noop=$(cd "$cold_replica" && find . -type f -exec sha256sum {} + | LC_ALL=C sort)
[[ "$before_noop" == "$after_noop" ]] || fail 'a no-op sync changed user-visible files'

printf '%s\n' 'published from the recovered client' >"$cold_replica/recovered-client.txt"
OFS_CONFIG="$cold_config" "$OFS_BIN" sync "$cold_alias" "$cold_replica" --state "$cold_state"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
diff -ru "$replica_a" "$cold_replica" || \
  fail 'the original client did not converge after the recovered client published'

printf '%s\n' 'acceptance: status exposes durable replica state without internals or secrets'
status_json=$(OFS_CONFIG="$cold_config" "$OFS_BIN" status --state "$cold_state" --json)
grep -Eq '"volume_model"[[:space:]]*:[[:space:]]*"managed"' <<<"$status_json" || \
  fail 'status did not report volume_model=managed'
grep -Eq '"access_model"[[:space:]]*:[[:space:]]*"sync"' <<<"$status_json" || \
  fail 'status did not report access_model=sync'
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
  fail 'status still reports conflicts after explicit resolution'
if grep -Eq '"capabilities"|"limitations"|"guarantee"|"metadata_authority"|"layout_settings"|"local_tree_operator"|"durable_state_owners"|"foreground_layout"|"storage_capabilities"' <<<"$status_json"; then
  fail 'status exposed assembly details or a static capability bundle'
fi

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

printf 'managed-sync acceptance passed (%s metadata)\n' "$OFS_METADATA_MODE"
