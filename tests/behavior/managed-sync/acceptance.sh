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

# One sanitized agent workspace is published as one generation.
mkdir -p "$tree_a/.agents/skills/storage" "$tree_a/.agents/empty" \
  "$tree_a/.bub" "$tree_a/.codex/history"
printf 'managed-sync\n' >"$tree_a/.agents/skills/storage/SKILL.md"
printf 'shared memory\n' >"$tree_a/.bub/memory.md"
printf '{"session":"a"}\n' >"$tree_a/.codex/history/session.jsonl"
printf 'theme = "plain"\n' >"$tree_a/config.toml"
"$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL"
"$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL"
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
diff -ru "$tree_a" "$tree_b"
status_json "$tree_a" >"$OFS_RUN_ROOT/status-a-1.json"
assert_status "$OFS_RUN_ROOT/status-a-1.json" base.generation '1'
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
assert_status "$OFS_RUN_ROOT/status-change-set.json" base.generation '2'

# Disjoint private and remote edits merge without losing either side.
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_a" >/dev/null
test -f "$tree_a/.bub/private.md"
assert_file "$tree_a/.bub/memory.md" 'remote memory'
"$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null
diff -ru "$tree_a" "$tree_b"

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

# Unsupported trees fail before a remote generation is changed.
status_json "$tree_b" >"$OFS_RUN_ROOT/status-before-reject.json"
before_reject=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base"]["generation"])' "$OFS_RUN_ROOT/status-before-reject.json")
ln -s config.toml "$tree_b/unsupported-link"
if "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$tree_b" >/dev/null 2>&1; then
  printf 'symbolic link unexpectedly synchronized\n' >&2
  exit 1
fi
rm "$tree_b/unsupported-link"
ln "$tree_b/config.toml" "$tree_b/hard-link"
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
export OFS_STORAGE_URL=${OFS_PUBLIC_STORAGE_URL:?}
status_json "$tree_a" >"$OFS_RUN_ROOT/status-offline.json"
assert_status "$OFS_RUN_ROOT/status-offline.json" remote.state '"unknown"'
python3 - "$OFS_RUN_ROOT/status-offline.json" <<'PY'
import json, sys
assert "generation" not in json.load(open(sys.argv[1]))["remote"]
PY
export OFS_STORAGE_URL=$credential_url

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

status_json "$tree_a" >"$OFS_RUN_ROOT/status-final.json"
assert_status "$OFS_RUN_ROOT/status-final.json" local '"clean"'
assert_status "$OFS_RUN_ROOT/status-final.json" publication '"idle"'
assert_status "$OFS_RUN_ROOT/status-final.json" conflicts '0'
printf 'Managed Sync scripted acceptance passed\n'
