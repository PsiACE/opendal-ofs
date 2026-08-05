#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0.

set -euo pipefail

: "${OFS_BIN:?} ${OFS_RUN_ROOT:?} ${OFS_VOLUME:?} ${OFS_STORAGE_URL:?}"
catalog="$OFS_RUN_ROOT/volumes.json"
metadata_args=()
if [[ -n ${OFS_METADATA_URL:-} ]]; then
  metadata_args=(--metadata "${OFS_METADATA_LOCATOR:-$OFS_METADATA_URL}")
fi

state_for() {
  local directory=$1
  printf '%s/.%s.ofs-state\n' "$(dirname "$directory")" "$(basename "$directory")"
}

sync_tree() {
  "$OFS_BIN" --config "$catalog" sync "$OFS_VOLUME" "$1" >/dev/null
}

source_tree="$OFS_RUN_ROOT/agent-source"
lagging_tree="$OFS_RUN_ROOT/agent-lagging"
mkdir "$source_tree" "$lagging_tree"
mkdir -p "$source_tree/memory" "$source_tree/skills"
printf 'shared memory\n' >"$source_tree/memory/shared.md"
printf 'managed-sync\n' >"$source_tree/skills/storage.txt"

"$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL" "${metadata_args[@]}"
if [[ -n ${OFS_WRONG_STORAGE_URL:-} ]]; then
  wrong_catalog="$OFS_RUN_ROOT/wrong-volumes.json"
  if "$OFS_BIN" --config "$wrong_catalog" volume create "$OFS_VOLUME" \
    --model managed --storage "$OFS_WRONG_STORAGE_URL" "${metadata_args[@]}" \
    >/dev/null 2>&1; then
    printf 'existing metadata scope accepted a different Data Store binding\n' >&2
    exit 1
  fi
  test ! -e "$wrong_catalog"
fi
sync_tree "$source_tree"
sync_tree "$lagging_tree"
diff -ru "$source_tree" "$lagging_tree"

# A new agent reads the current generation, becomes the only publisher, then
# the previous agent and its private state disappear.
current_tree=$source_tree
for round in $(seq 1 12); do
  next_tree="$OFS_RUN_ROOT/agent-$round"
  mkdir "$next_tree"
  sync_tree "$next_tree"
  diff -ru "$current_tree" "$next_tree"
  printf 'experience %02d\n' "$round" >"$next_tree/memory/experience-$round.md"
  sync_tree "$next_tree"
  rm -rf "$current_tree" "$(state_for "$current_tree")"
  current_tree=$next_tree
done

# A reader left at generation one consumes the complete incremental range.
sync_tree "$lagging_tree"
diff -ru "$current_tree" "$lagging_tree"

# Losing the local volume definition does not lose the remote Managed Volume.
rm "$catalog"
"$OFS_BIN" --config "$catalog" volume create "$OFS_VOLUME" \
  --model managed --storage "$OFS_STORAGE_URL" "${metadata_args[@]}"
cold_tree="$OFS_RUN_ROOT/agent-cold"
mkdir "$cold_tree"
sync_tree "$cold_tree"
diff -ru "$current_tree" "$cold_tree"

# Losing both a local tree and its replica state produces the same cold result.
rm -rf "$lagging_tree" "$(state_for "$lagging_tree")"
mkdir "$lagging_tree"
sync_tree "$lagging_tree"
diff -ru "$current_tree" "$lagging_tree"

"$OFS_BIN" --config "$catalog" status "$cold_tree" --json \
  >"$OFS_RUN_ROOT/status-final.json"
python3 - "$OFS_RUN_ROOT/status-final.json" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1], encoding="utf-8"))
assert status["local"] == "clean", status
assert status["base"]["generation"] == 13, status
assert status["remote"] == {"state": "at_base", "generation": 13}, status
assert status["publication"] == "idle", status
assert status["materialize"] == "idle", status
assert status["conflicts"] == 0, status
PY
if grep -q '"token"' "$catalog"; then
  printf 'catalog persisted a Metadata Store credential\n' >&2
  exit 1
fi
printf 'Managed Sync agent lifecycle acceptance passed\n'
