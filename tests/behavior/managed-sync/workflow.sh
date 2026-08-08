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
cold_config="$OFS_CASE_ROOT/cold-client/config.json"
direct_config="$OFS_CASE_ROOT/direct-client/config.json"
replica_a="$OFS_CASE_ROOT/replica-a"
replica_b="$OFS_CASE_ROOT/replica-b"
cold_replica="$OFS_CASE_ROOT/cold-replica"
state_a="$OFS_CASE_ROOT/state/replica-a.json"
state_b="$OFS_CASE_ROOT/state/replica-b.json"
cold_state="$OFS_CASE_ROOT/state/cold-replica.json"

mkdir -p "$(dirname "$config")" "$(dirname "$cold_config")" "$(dirname "$direct_config")" \
  "$replica_a" "$replica_b" "$cold_replica" "$(dirname "$state_a")"

printf '%s\n' 'acceptance: expose only named volume access commands'
cli_help=$("$OFS_BIN" --help)
grep -Eq '^  mount[[:space:]]' <<<"$cli_help" || fail 'help omitted the Mount access command'
grep -Eq '^  sync[[:space:]]' <<<"$cli_help" || fail 'help omitted the Sync access command'
if grep -Eq 'MOUNT_PATH.*BACKEND_URL|OFS_MOUNT_PATH|OFS_BACKEND' <<<"$cli_help"; then
  fail 'help still advertises the obsolete positional Direct Mount form'
fi
direct_create=$(OFS_CONFIG="$direct_config" "$OFS_BIN" volume create archive \
  --model direct --storage 'memory:///acceptance')
grep -Fq 'created direct volume "archive"' <<<"$direct_create" || \
  fail 'named Direct volume creation did not report its result'
direct_reopen=$(OFS_CONFIG="$direct_config" "$OFS_BIN" volume create archive \
  --model direct --storage 'memory:///acceptance')
grep -Fq 'opened direct volume "archive"' <<<"$direct_reopen" || \
  fail 'named Direct volume creation was not idempotent'
if direct_sync_error=$(OFS_CONFIG="$direct_config" "$OFS_BIN" sync archive "$cold_replica" \
  --state "$OFS_CASE_ROOT/state/direct.json" 2>&1); then
  fail 'Direct Sync started even though that access combination is unavailable'
fi
grep -Fq 'sync requires a Managed volume' <<<"$direct_sync_error" || \
  fail 'unavailable Direct Sync did not report an actionable admission error'

volume_create=(volume create workspace --model managed --storage "$OFS_STORAGE_URL")
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  volume_create+=(--metadata "$OFS_METADATA_URL")
fi

printf '%s\n' 'acceptance: create and reopen one managed volume'
OFS_CONFIG="$config" "$OFS_BIN" "${volume_create[@]}"
OFS_CONFIG="$config" "$OFS_BIN" "${volume_create[@]}"
if managed_mount_error=$(OFS_CONFIG="$config" "$OFS_BIN" mount workspace "$cold_replica" 2>&1); then
  fail 'Managed Mount started even though that access combination is unavailable'
fi
grep -Fq 'mount currently supports Direct volumes' <<<"$managed_mount_error" || \
  fail 'unavailable Managed Mount did not report an actionable admission error'

printf '%s\n' 'private before sync' >"$replica_a/first.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/first.txt" ]] || fail 'volume creation published a local file without sync'

printf '%s\n' 'acceptance: first publication and empty-replica materialization'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
cmp "$replica_a/first.txt" "$replica_b/first.txt" || fail 'empty replica did not materialize first.txt'

printf '%s\n' 'acceptance: reject hard links before publication'
printf '%s\n' 'must remain local' >"$replica_a/hard-link-source.txt"
if ln "$replica_a/hard-link-source.txt" "$replica_a/hard-link-alias.txt" 2>/dev/null; then
  before_hard_link=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_a" --state "$state_a" --json)
  if hard_link_error=$(OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a" 2>&1); then
    fail 'hard-linked files were published'
  fi
  grep -Fq 'hard link' <<<"$hard_link_error" || fail 'hard-link rejection was not explicit'
  after_hard_link=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_a" --state "$state_a" --json)
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
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
printf '%s\n' 'acceptance: pack live small content and repeat idempotently'
first_pack=$(OFS_CONFIG="$config" "$OFS_BIN" volume pack workspace)
grep -Eq 'packs=[1-9][0-9]*' <<<"$first_pack" || fail 'first pack run produced no pack'
grep -Eq 'content=[1-9][0-9]*' <<<"$first_pack" || fail 'first pack run indexed no content'
rebuilt_index=$(OFS_CONFIG="$config" "$OFS_BIN" volume pack workspace --rebuild-index)
grep -Eq 'rebuilt_index_content=[1-9][0-9]*' <<<"$rebuilt_index" || \
  fail 'pack index rebuild did not recover verified content locations'
second_pack=$(OFS_CONFIG="$config" "$OFS_BIN" volume pack workspace)
grep -Fq 'packs=0 content=0 logical_bytes=0' <<<"$second_pack" || \
  fail 'second pack run was not idempotent'
cp "$replica_a/first.txt" "$replica_a/reused-from-pack.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
cmp "$replica_a/reused-from-pack.txt" "$replica_b/reused-from-pack.txt" || \
  fail 'authority-known packed content did not round trip at a new path'
[[ -d "$replica_b/nested/level" ]] || fail 'nested directories were not materialized'
cmp "$replica_a/nested/level/entry.txt" "$replica_b/nested/level/entry.txt" || \
  fail 'nested file content did not round trip'
[[ ! -s "$replica_b/empty.bin" ]] || fail 'empty file did not round trip'
cmp "$replica_a/large.bin" "$replica_b/large.bin" || fail 'large file did not round trip'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$replica_b/tools/run.sh" ]] || fail 'executable bit did not round trip'
fi

printf '%s\n' 'acceptance: modify, rename, and delete remote entries'
printf '%s\n' 'modified before rename' >"$replica_a/nested/level/entry.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
grep -Fxq 'modified before rename' "$replica_b/nested/level/entry.txt" || \
  fail 'remote file modification was not materialized'
mv "$replica_a/nested/level/entry.txt" "$replica_a/nested/renamed.txt"
rm "$replica_a/nested/level/removed.txt"
rmdir "$replica_a/nested/level"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/nested/level" ]] || fail 'deleted remote directory remained locally'
[[ ! -e "$replica_b/nested/level/entry.txt" ]] || fail 'old remote rename path remained locally'
[[ ! -e "$replica_b/nested/level/removed.txt" ]] || fail 'deleted remote file remained locally'
grep -Fxq 'modified before rename' "$replica_b/nested/renamed.txt" || \
  fail 'remote file rename was not materialized'

printf '%s\n' 'acceptance: preserve a moved directory tree'
mkdir -p "$replica_a/tree-before/branch/empty"
printf '%s\n' 'directory identity survives a move' >"$replica_a/tree-before/branch/leaf.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
mv "$replica_a/tree-before" "$replica_a/tree-after"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
[[ ! -e "$replica_b/tree-before" ]] || fail 'old directory move path remained remotely'
[[ -d "$replica_b/tree-after/branch/empty" ]] || fail 'moved empty directory was not materialized'
grep -Fxq 'directory identity survives a move' "$replica_b/tree-after/branch/leaf.txt" || \
  fail 'moved directory subtree content was not materialized'

printf '%s\n' 'published by replica a' >"$replica_a/a-only.txt"
printf '%s\n' 'published by replica b' >"$replica_b/b-only.txt"
printf '%s\n' 'acceptance: merge disjoint changes from two replicas'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
cmp "$replica_a/a-only.txt" "$replica_b/a-only.txt" || fail 'replica b lost replica a disjoint change'
cmp "$replica_a/b-only.txt" "$replica_b/b-only.txt" || fail 'replica a lost replica b disjoint change'

printf '%s\n' 'common base' >"$replica_a/shared.txt"
printf '%s\n' 'second common base' >"$replica_a/shared-two.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
printf '%s\n' 'candidate from replica a' >"$replica_a/shared.txt"
printf '%s\n' 'second candidate from replica a' >"$replica_a/shared-two.txt"
printf '%s\n' 'candidate from replica b' >"$replica_b/shared.txt"
printf '%s\n' 'second candidate from replica b' >"$replica_b/shared-two.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a"

printf '%s\n' 'acceptance: retain and report same-path conflicts'
if OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"; then
  fail 'same-path concurrent edits succeeded without an explicit resolution'
fi
grep -Fxq 'candidate from replica a' "$replica_a/shared.txt" || fail 'remote conflict candidate was lost'
grep -Fxq 'candidate from replica b' "$replica_b/shared.txt" || fail 'local conflict candidate was lost'
grep -Fxq 'second candidate from replica a' "$replica_a/shared-two.txt" || \
  fail 'second remote conflict candidate was lost'
grep -Fxq 'second candidate from replica b' "$replica_b/shared-two.txt" || \
  fail 'second local conflict candidate was lost'
conflict_status=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_b" --state "$state_b" --json)
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*2' <<<"$conflict_status" || \
  fail 'status did not report both unresolved conflicts'

printf '%s\n' 'acceptance: resolve multiple conflicts explicitly with retained local candidates'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b" \
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
  recovery_status=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_a" --state "$state_a" --json 2>/dev/null || true)
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
recovery_status=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_a" --state "$state_a" --json)
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
recovery_status=$(OFS_CONFIG="$config" "$OFS_BIN" status "$replica_a" --state "$state_a" --json)
grep -Eq '"pending"[[:space:]]*:[[:space:]]*false' <<<"$recovery_status" || \
  fail 'recovered publication did not clear its completed intent'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
cmp "$replica_a/crash-recovery/128.bin" "$replica_b/crash-recovery/128.bin" || \
  fail 'recovered publication did not materialize on another replica'

printf '%s\n' 'acceptance: recover through a long bounded change history'
for generation in $(seq 1 60); do
  printf 'history change %s\n' "$generation" >"$replica_a/checkpoint.txt"
  OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_a" --state "$state_a" >/dev/null
done
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$replica_b" --state "$state_b"
grep -Fxq 'history change 60' "$replica_b/checkpoint.txt" || \
  fail 'replica did not recover the fixed target after a long change history'
garbage_collection=$(OFS_CONFIG="$config" "$OFS_BIN" volume gc workspace)
grep -Eq 'deleted=[1-9][0-9]*' <<<"$garbage_collection" || \
  fail 'namespace-fenced collection removed no unreachable loose data'

printf '%s\n' 'acceptance: rebuild a cold client from remote authority'
OFS_CONFIG="$cold_config" "$OFS_BIN" "${volume_create[@]}"
OFS_CONFIG="$cold_config" "$OFS_BIN" sync workspace "$cold_replica" --state "$cold_state"
diff -ru "$replica_a" "$cold_replica" || fail 'cold replica does not match the published tree'
if [[ "$(uname -s)" != MINGW* && "$(uname -s)" != MSYS* ]]; then
  [[ -x "$cold_replica/tools/run.sh" ]] || fail 'cold rebuild lost executable bit'
fi

before_noop=$(cd "$cold_replica" && find . -type f -exec sha256sum {} + | LC_ALL=C sort)
OFS_CONFIG="$cold_config" "$OFS_BIN" sync workspace "$cold_replica" --state "$cold_state"
after_noop=$(cd "$cold_replica" && find . -type f -exec sha256sum {} + | LC_ALL=C sort)
[[ "$before_noop" == "$after_noop" ]] || fail 'a no-op sync changed user-visible files'

printf '%s\n' 'acceptance: status exposes the selected models without secrets'
status_json=$(OFS_CONFIG="$cold_config" "$OFS_BIN" status "$cold_replica" --state "$cold_state" --json)
grep -Eq '"volume_model"[[:space:]]*:[[:space:]]*"managed"' <<<"$status_json" || \
  fail 'status did not report volume_model=managed'
grep -Eq '"access_model"[[:space:]]*:[[:space:]]*"sync"' <<<"$status_json" || \
  fail 'status did not report access_model=sync'
grep -Eq '"metadata_authority"[[:space:]]*:[[:space:]]*"(object|d1)"' <<<"$status_json" || \
  fail 'status did not report its metadata authority'
grep -Fq '"local_tree_operator":"opendal_fs"' <<<"$status_json" || \
  fail 'status did not report OpenDAL fs local I/O'
grep -Fq '"durable_state_owners":' <<<"$status_json" || \
  fail 'status omitted durable state ownership'
grep -Fq '"foreground_layout":"whole"' <<<"$status_json" || \
  fail 'status did not report the default file layout'
grep -Eq '"conflicts"[[:space:]]*:[[:space:]]*0' <<<"$status_json" || \
  fail 'status still reports conflicts after explicit resolution'
if grep -Eq '"capabilities"|"limitations"|"guarantee"' <<<"$status_json"; then
  fail 'status exposed a static Managed-Sync capability bundle'
fi
grep -Fq -- '"storage_capabilities":' <<<"$status_json" || \
  fail 'status omitted observed storage capabilities'

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
