#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0.

set -euo pipefail

actor=${1:-scripted}
: "${OFS_BIN:?} ${OFS_RUN_ROOT:?} ${OFS_VOLUME:?} ${OFS_STORAGE_URL:?}"
catalog="$OFS_RUN_ROOT/volumes.json"
tree_a="$OFS_RUN_ROOT/agent-a"
tree_b="$OFS_RUN_ROOT/agent-b"
tree_c="$OFS_RUN_ROOT/agent-c"
mkdir "$tree_a" "$tree_b" "$tree_c"
metadata_args=()
if [[ -n ${OFS_METADATA_URL:-} ]]; then
  metadata_args=(--metadata "${OFS_METADATA_LOCATOR:-$OFS_METADATA_URL}")
fi

status_json() {
  "$OFS_BIN" --config "$catalog" status "$1" --json
}

assert_status() {
  local file=$1 path=$2 expected=$3
  python3 - "$file" "$path" "$expected" <<'PY'
import json
import sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[int(part)] if part.isdigit() else value[part]
expected = json.loads(sys.argv[3])
assert value == expected, (sys.argv[2], value, expected)
PY
}

assert_file() {
  test "$(cat "$1")" = "$2"
}

if [[ $actor == bub ]]; then
  : "${OPENROUTER_API_KEY:?Bub acceptance requires OPENROUTER_API_KEY}"
  command -v bub >/dev/null
  export OFS_CONFIG="$catalog"
  export OFS_SANDBOX_A="$tree_a" OFS_SANDBOX_B="$tree_b" OFS_SANDBOX_C="$tree_c"
  export BUB_HOME="$OFS_RUN_ROOT/bub"
  export BUB_API_KEY="$OPENROUTER_API_KEY"
  export BUB_OPENROUTER_API_KEY="$OPENROUTER_API_KEY"
  export BUB_MODEL="${BUB_MODEL:-openrouter:deepseek/deepseek-v4-flash-0731}"
  export BUB_MAX_STEPS="${BUB_MAX_STEPS:-80}"
  task=$(<"$(dirname "$0")/agent-task.md")
  timeout --signal=TERM --kill-after=15s "${BUB_TIMEOUT_SECONDS:-840}s" \
    bub --workspace "$OFS_RUN_ROOT" run --session-id "$OFS_VOLUME" "$task" \
    >"$OFS_RUN_ROOT/bub.log" 2>&1
  if grep -Fq -- "$OFS_STORAGE_URL" "$OFS_RUN_ROOT/bub.log"; then
    printf 'Bub log exposed the credentialed storage URL\n' >&2
    exit 1
  fi
  assert_file "$tree_a/memory/shared.md" 'shared memory from agent'
  assert_file "$tree_a/memory/private.md" 'private draft from agent'
  assert_file "$tree_b/skills/storage.txt" 'managed-sync'
  test ! -e "$tree_b/memory/private.md"
  test ! -e "$tree_c/memory/private.md"
  status_json "$tree_a" >"$OFS_RUN_ROOT/status-a.json"
  status_json "$tree_b" >"$OFS_RUN_ROOT/status-b.json"
  status_json "$tree_c" >"$OFS_RUN_ROOT/status-c.json"
  assert_status "$OFS_RUN_ROOT/status-a.json" local '"changed"'
  for file in "$OFS_RUN_ROOT/status-b.json" "$OFS_RUN_ROOT/status-c.json"; do
    assert_status "$file" local '"clean"'
    assert_status "$file" publication '"idle"'
    assert_status "$file" conflicts '0'
  done
  printf 'Managed Sync Bub acceptance passed\n'
  exit 0
fi

if [[ $actor == recovery ]]; then
  source_tree=$tree_a
  target_tree=$tree_b
  "$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
    --model managed --storage "$OFS_STORAGE_URL" "${metadata_args[@]}"
  mkdir "$source_tree/payload"
  for index in $(seq 0 63); do
    file="$source_tree/payload/file-$(printf '%02d' "$index").bin"
    dd if=/dev/zero of="$file" bs=1M count=2 status=none
    printf '%08d' "$index" | dd of="$file" bs=8 count=1 conv=notrunc status=none
  done
  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$source_tree" >/dev/null

  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$target_tree" \
    >"$OFS_RUN_ROOT/interrupted.log" 2>&1 & apply_pid=$!
  partial=0
  for _ in $(seq 1 2000); do
    if [[ -d $target_tree/payload ]]; then
      count=$(find "$target_tree/payload" -type f | wc -l)
    else
      count=0
    fi
    if ((count > 0 && count < 64)); then
      partial=$count
      kill -9 "$apply_pid"
      break
    fi
    if ! kill -0 "$apply_pid" 2>/dev/null; then break; fi
    sleep 0.005
  done
  set +e
  wait "$apply_pid" 2>/dev/null
  interrupted_rc=$?
  set -e
  if ((partial == 0 || interrupted_rc == 0)); then
    printf 'materialization did not expose an interruptible partial tree\n' >&2
    exit 1
  fi

  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$target_tree" >/dev/null
  diff -ru "$source_tree" "$target_tree"
  status_json "$target_tree" >"$OFS_RUN_ROOT/recovered.status.json"
  assert_status "$OFS_RUN_ROOT/recovered.status.json" base.generation '1'
  assert_status "$OFS_RUN_ROOT/recovered.status.json" publication '"idle"'
  assert_status "$OFS_RUN_ROOT/recovered.status.json" materialize '"idle"'
  assert_status "$OFS_RUN_ROOT/recovered.status.json" conflicts '0'

  # Kill after MinIO has committed the head but before the durable intent clears.
  trace_container="ofs-recovery-trace-${PPID}-$$"
  trace_file="$OFS_RUN_ROOT/minio-trace.jsonl"
  trace_runtime=${OFS_CONTAINER_RUNTIME:?}
  "$trace_runtime" run --rm --name "$trace_container" --network host --entrypoint /bin/sh \
    quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z -c \
    "mc alias set recovery ${OFS_MINIO_ENDPOINT:?} ofs-acceptance ofs-acceptance-password >/dev/null && mc admin trace --json recovery" \
    >"$trace_file" 2>/dev/null & trace_pid=$!
  sleep 0.3
  printf 'changed\n' | dd of="$source_tree/payload/file-00.bin" \
    bs=8 count=1 conv=notrunc status=none
  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$source_tree" \
    >"$OFS_RUN_ROOT/unknown.log" 2>&1 & publish_pid=$!
  killed=false
  for _ in $(seq 1 2000); do
    if grep -F 's3.PutObject' "$trace_file" | grep -Eq 'metadata(%2F|/)head'; then
      if kill -0 "$publish_pid" 2>/dev/null; then
        kill -9 "$publish_pid"
        killed=true
      fi
      break
    fi
    if ! kill -0 "$publish_pid" 2>/dev/null; then break; fi
    sleep 0.005
  done
  "$trace_runtime" rm -f "$trace_container" >/dev/null 2>&1 || true
  wait "$trace_pid" 2>/dev/null || true
  set +e
  wait "$publish_pid" 2>/dev/null
  unknown_rc=$?
  set -e
  if ! $killed || ((unknown_rc == 0)); then
    printf 'publication did not retain an interruptible unknown result\n' >&2
    exit 1
  fi
  status_json "$source_tree" >"$OFS_RUN_ROOT/unknown.status.json"
  assert_status "$OFS_RUN_ROOT/unknown.status.json" base.generation '1'
  assert_status "$OFS_RUN_ROOT/unknown.status.json" remote.generation '2'
  assert_status "$OFS_RUN_ROOT/unknown.status.json" publication '"pending"'

  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$source_tree" >/dev/null
  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$target_tree" >/dev/null
  diff -ru "$source_tree" "$target_tree"
  status_json "$source_tree" >"$OFS_RUN_ROOT/resolved-unknown.status.json"
  assert_status "$OFS_RUN_ROOT/resolved-unknown.status.json" base.generation '2'
  assert_status "$OFS_RUN_ROOT/resolved-unknown.status.json" publication '"idle"'
  printf 'Managed Sync recovery acceptance passed\n'
  exit 0
fi

# Two empty replicas first establish G0, then independently initialize the same
# standard agent directories before either one publishes.
"$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL" "${metadata_args[@]}"
"$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL" "${metadata_args[@]}"
if grep -q 'ofs-acceptance-password' "$catalog"; then
  printf 'catalog persisted a storage credential\n' >&2
  exit 1
fi
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_c" \
  --require hard-link >/dev/null 2>&1; then
  printf 'sync admitted an unavailable required capability\n' >&2
  exit 1
fi
test ! -e "$OFS_RUN_ROOT/.agent-c.ofs-state"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
mkdir -p "$tree_a/.agents/skills/storage" "$tree_a/.agents/empty" \
  "$tree_a/.bub" "$tree_a/.codex/history"
mkdir -p "$tree_b/.agents/skills/reviewer" "$tree_b/.agents/empty" \
  "$tree_b/.bub" "$tree_b/.codex/history"
printf 'managed-sync\n' >"$tree_a/.agents/skills/storage/SKILL.md"
printf 'reviewer\n' >"$tree_b/.agents/skills/reviewer/SKILL.md"
printf 'shared memory\n' >"$tree_a/.bub/memory.md"
printf 'peer memory\n' >"$tree_b/.bub/peer.md"
printf '{"session":"a"}\n' >"$tree_a/.codex/history/session.jsonl"
printf '{"session":"b"}\n' >"$tree_b/.codex/history/peer.jsonl"
printf 'theme = "plain"\n' >"$tree_a/config.toml"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
diff -ru "$tree_a" "$tree_b"
status_json "$tree_a" >"$OFS_RUN_ROOT/status-a-1.json"
assert_status "$OFS_RUN_ROOT/status-a-1.json" base.generation '2'
assert_status "$OFS_RUN_ROOT/status-a-1.json" remote.state '"at_base"'

# Two non-empty trees without a durable common base never guess an ancestor.
tree_unbound="$OFS_RUN_ROOT/unbound"
mkdir "$tree_unbound"
printf 'unpublished candidate\n' >"$tree_unbound/local.txt"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_unbound" >/dev/null 2>&1; then
  printf 'unbound non-empty tree unexpectedly synchronized\n' >&2
  exit 1
fi
assert_file "$tree_unbound/local.txt" 'unpublished candidate'

# An ordinary local edit remains private until that replica explicitly syncs.
printf 'private draft\n' >"$tree_a/.bub/private.md"
status_json "$tree_a" >"$OFS_RUN_ROOT/status-private.json"
assert_status "$OFS_RUN_ROOT/status-private.json" local '"changed"'
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
test ! -e "$tree_b/.bub/private.md"

# Create, modify, delete, rename and an empty directory advance exactly once.
printf 'remote memory\n' >"$tree_b/.bub/memory.md"
mv "$tree_b/.agents/skills/storage/SKILL.md" "$tree_b/.agents/skills/storage/REFERENCE.md"
rm "$tree_b/.codex/history/session.jsonl"
printf 'new session\n' >"$tree_b/.codex/history/new.jsonl"
mkdir "$tree_b/.codex/cache"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
status_json "$tree_b" >"$OFS_RUN_ROOT/status-change-set.json"
assert_status "$OFS_RUN_ROOT/status-change-set.json" base.generation '3'

# Disjoint private and remote edits merge without losing either side.
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
test -f "$tree_a/.bub/private.md"
assert_file "$tree_a/.bub/memory.md" 'remote memory'
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
diff -ru "$tree_a" "$tree_b"

# A file changing throughout preparation cannot advance the remote generation.
status_json "$tree_b" >"$OFS_RUN_ROOT/status-before-unstable.json"
before_unstable=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base"]["generation"])' "$OFS_RUN_ROOT/status-before-unstable.json")
dd if=/dev/zero of="$tree_a/.bub/unstable.bin" bs=1M count=32 status=none
set +e
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  >"$OFS_RUN_ROOT/unstable.log" 2>&1 & unstable_pid=$!
(
  update=0
  while kill -0 "$unstable_pid" 2>/dev/null; do
    printf '%08d' "$update" | dd of="$tree_a/.bub/unstable.bin" \
      bs=8 count=1 conv=notrunc status=none
    update=$((update + 1))
  done
) & modifier_pid=$!
wait "$unstable_pid"; unstable_rc=$?
wait "$modifier_pid"
set -e
if ((unstable_rc == 0)); then
  printf 'continuously changing source unexpectedly published\n' >&2
  exit 1
fi
status_json "$tree_b" >"$OFS_RUN_ROOT/status-after-unstable.json"
assert_status "$OFS_RUN_ROOT/status-after-unstable.json" remote.generation "$before_unstable"
rm "$tree_a/.bub/unstable.bin"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null

# Competing edits retain a conflict; the selected local shape publishes later.
printf 'candidate a\n' >"$tree_a/.bub/memory.md"
printf 'winner b\n' >"$tree_b/.bub/memory.md"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  >"$OFS_RUN_ROOT/conflict.log" 2>&1; then
  printf 'same-path conflict unexpectedly published\n' >&2
  exit 1
fi
assert_file "$tree_a/.bub/memory.md" 'candidate a'
status_json "$tree_a" >"$OFS_RUN_ROOT/status-conflict.json"
assert_status "$OFS_RUN_ROOT/status-conflict.json" conflicts '1'
assert_status "$OFS_RUN_ROOT/status-conflict.json" publication '"conflict"'
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  --resolve .bub/memory.md >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
diff -ru "$tree_a" "$tree_b"

# Divergent renames retain one identity conflict and resolve to the local shape.
mv "$tree_a/config.toml" "$tree_a/config-a.toml"
mv "$tree_b/config.toml" "$tree_b/config-b.toml"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null 2>&1; then
  printf 'divergent rename unexpectedly published\n' >&2
  exit 1
fi
test -f "$tree_a/config-a.toml"
test ! -e "$tree_a/config-b.toml"
status_json "$tree_a" >"$OFS_RUN_ROOT/status-rename-conflict.json"
assert_status "$OFS_RUN_ROOT/status-rename-conflict.json" conflict_records.0.kind '"divergent_rename"'
rename_conflict_path=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["conflict_records"][0]["path"])' "$OFS_RUN_ROOT/status-rename-conflict.json")
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  --resolve "$rename_conflict_path" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
diff -ru "$tree_a" "$tree_b"

# Delete-versus-modify retains local absence until it is explicitly selected.
rm "$tree_a/.codex/history/new.jsonl"
printf 'remote edit\n' >"$tree_b/.codex/history/new.jsonl"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null 2>&1; then
  printf 'delete-versus-modify unexpectedly published\n' >&2
  exit 1
fi
test ! -e "$tree_a/.codex/history/new.jsonl"
status_json "$tree_a" >"$OFS_RUN_ROOT/status-delete-conflict.json"
assert_status "$OFS_RUN_ROOT/status-delete-conflict.json" conflict_records.0.kind '"delete_vs_modify"'
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  --resolve .codex/history/new.jsonl >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
diff -ru "$tree_a" "$tree_b"

# Unsupported trees fail before a remote generation is changed.
status_json "$tree_b" >"$OFS_RUN_ROOT/status-before-reject.json"
before_reject=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base"]["generation"])' "$OFS_RUN_ROOT/status-before-reject.json")
ln -s config.toml "$tree_b/unsupported-link"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null 2>&1; then
  printf 'symbolic link unexpectedly synchronized\n' >&2
  exit 1
fi
rm "$tree_b/unsupported-link"
ln "$tree_b/config-a.toml" "$tree_b/hard-link"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null 2>&1; then
  printf 'hard link unexpectedly synchronized\n' >&2
  exit 1
fi
rm "$tree_b/hard-link"
printf 'reserved\n' >"$tree_b/CON.txt"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null 2>&1; then
  printf 'reserved portable name unexpectedly synchronized\n' >&2
  exit 1
fi
rm "$tree_b/CON.txt"
mkdir "$tree_b/collision"
printf 'upper\n' >"$tree_b/collision/Skill"
printf 'lower\n' >"$tree_b/collision/skill"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null 2>&1; then
  printf 'portable case collision unexpectedly synchronized\n' >&2
  exit 1
fi
rm -rf "$tree_b/collision"
non_nfc=$'cafe\u0301'
printf 'decomposed\n' >"$tree_b/$non_nfc"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null 2>&1; then
  printf 'non-NFC name unexpectedly synchronized\n' >&2
  exit 1
fi
rm "$tree_b/$non_nfc"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" \
  --state "$OFS_RUN_ROOT/.agent-a.ofs-state" >/dev/null 2>&1; then
  printf 'replica state binding mismatch unexpectedly synchronized\n' >&2
  exit 1
fi
status_json "$tree_b" >"$OFS_RUN_ROOT/status-after-reject.json"
assert_status "$OFS_RUN_ROOT/status-after-reject.json" base.generation "$before_reject"

# A fresh tree and a locally removed replica both cold-rebuild exactly.
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_c" >/dev/null
diff -ru "$tree_a" "$tree_c"
recovered_catalog="$OFS_RUN_ROOT/recovered-volumes.json"
recovered_tree="$OFS_RUN_ROOT/recovered-agent"
mkdir "$recovered_tree"
"$OFS_BIN" --config "$recovered_catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL" "${metadata_args[@]}"
"$OFS_BIN" --config "$recovered_catalog" sync "$OFS_VOLUME" "$recovered_tree" >/dev/null
diff -ru "$tree_a" "$recovered_tree"
if grep -q 'ofs-acceptance-password' "$recovered_catalog"; then
  printf 'recovered catalog persisted a storage credential\n' >&2
  exit 1
fi
rm -rf "$tree_b" "$OFS_RUN_ROOT/.agent-b.ofs-state"
mkdir "$tree_b"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
diff -ru "$tree_a" "$tree_b"

# Status is read-only and reports unknown rather than a cached remote value.
state_file="$OFS_RUN_ROOT/.agent-a.ofs-state/state.json"
before_status=$(sha256sum "$state_file" | cut -d' ' -f1)
status_json "$tree_a" >"$OFS_RUN_ROOT/status-read-only.json"
test "$before_status" = "$(sha256sum "$state_file" | cut -d' ' -f1)"
credential_url=$OFS_STORAGE_URL
credential_metadata=${OFS_METADATA_URL:-}
if ((${#metadata_args[@]})); then
  unset OFS_METADATA_URL
else
  export OFS_STORAGE_URL=${OFS_PUBLIC_STORAGE_URL:?}
fi
status_json "$tree_a" >"$OFS_RUN_ROOT/status-offline.json"
assert_status "$OFS_RUN_ROOT/status-offline.json" remote.state '"unknown"'
python3 - "$OFS_RUN_ROOT/status-offline.json" <<'PY'
import json, sys
remote = json.load(open(sys.argv[1]))["remote"]
assert "generation" not in remote
assert remote["error"]["kind"]
assert remote["error"]["message"]
PY
export OFS_STORAGE_URL=$credential_url
if [[ -n $credential_metadata ]]; then
  export OFS_METADATA_URL=$credential_metadata
fi

# Independent publishers may race or serialize, but neither change is lost.
printf 'agent a\n' >"$tree_a/.agents/a.txt"
printf 'agent b\n' >"$tree_b/.agents/b.txt"
set +e
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >"$OFS_RUN_ROOT/race-a.log" 2>&1 & pid_a=$!
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >"$OFS_RUN_ROOT/race-b.log" 2>&1 & pid_b=$!
wait "$pid_a"; race_a=$?
wait "$pid_b"; race_b=$?
set -e
if ((race_a != 0 && race_b != 0)); then
  printf 'both independent publishers failed\n' >&2
  exit 1
fi
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  >"$OFS_RUN_ROOT/race-a-reconcile.log" 2>&1
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" \
  >"$OFS_RUN_ROOT/race-b-reconcile.log" 2>&1
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" \
  >"$OFS_RUN_ROOT/race-a-catch-up.log" 2>&1
test -f "$tree_a/.agents/a.txt" -a -f "$tree_a/.agents/b.txt"
diff -ru "$tree_a" "$tree_b"

# A replica can materialize directory removals and then publish the next
# generation's directory delete/add set after a complete catch-up.
mkdir -p "$tree_a/.agents/handoff/remove-a"
printf 'first publisher\n' >"$tree_a/.agents/handoff/remove-a/value.txt"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
rm -rf "$tree_a/.agents/handoff/remove-a"
mkdir -p "$tree_a/.agents/handoff/add-a"
printf 'published by a\n' >"$tree_a/.agents/handoff/add-a/value.txt"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
rm -rf "$tree_b/.agents/handoff/add-a"
mkdir -p "$tree_b/.agents/handoff/add-b"
printf 'published by b\n' >"$tree_b/.agents/handoff/add-b/value.txt"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
diff -ru "$tree_a" "$tree_b"

# An upgrade may make independent replicas create the same missing nested
# public directories. Their directory identities coalesce and disjoint files
# are both retained.
mkdir -p "$tree_a/.agents/concurrent-upgrade/skills"
mkdir -p "$tree_b/.agents/concurrent-upgrade/skills"
printf 'from a\n' >"$tree_a/.agents/concurrent-upgrade/skills/a.md"
printf 'from b\n' >"$tree_b/.agents/concurrent-upgrade/skills/b.md"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
test -f "$tree_a/.agents/concurrent-upgrade/skills/a.md"
test -f "$tree_a/.agents/concurrent-upgrade/skills/b.md"
diff -ru "$tree_a" "$tree_b"

# The same rule applies after all replicas catch up to deletion of a public
# directory and independently recreate it.
rm -rf "$tree_a/.agents/concurrent-upgrade"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
mkdir -p "$tree_a/.agents/concurrent-upgrade/skills"
mkdir -p "$tree_b/.agents/concurrent-upgrade/skills"
printf 'recreated by a\n' >"$tree_a/.agents/concurrent-upgrade/skills/a.md"
printf 'recreated by b\n' >"$tree_b/.agents/concurrent-upgrade/skills/b.md"
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
test -f "$tree_a/.agents/concurrent-upgrade/skills/a.md"
test -f "$tree_a/.agents/concurrent-upgrade/skills/b.md"
diff -ru "$tree_a" "$tree_b"

status_json "$tree_a" >"$OFS_RUN_ROOT/status-final.json"
assert_status "$OFS_RUN_ROOT/status-final.json" local '"clean"'
assert_status "$OFS_RUN_ROOT/status-final.json" publication '"idle"'
assert_status "$OFS_RUN_ROOT/status-final.json" conflicts '0'
printf 'Managed Sync scripted acceptance passed\n'
