#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

fail() {
  printf 'managed-branch acceptance: %s\n' "$*" >&2
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

[[ -x "$OFS_BIN" ]] || fail 'OFS_BIN must name the built ofs executable'
[[ -n "$OFS_CASE_ROOT" ]] || fail 'OFS_CASE_ROOT must name a fresh test directory'
[[ ! -e "$OFS_CASE_ROOT" ]] || fail "OFS_CASE_ROOT already exists: $OFS_CASE_ROOT"
[[ -n "$OFS_STORAGE_URL" ]] || fail 'OFS_STORAGE_URL must be a credential-free data URL'

case "$OFS_METADATA_MODE" in
  object)
    [[ -z "$OFS_METADATA_URL" ]] || fail 'object metadata uses OFS_STORAGE_URL'
    ;;
  d1)
    [[ -n "$OFS_METADATA_URL" ]] || fail 'd1 metadata requires OFS_METADATA_URL'
    ;;
  *)
    fail "OFS_METADATA_MODE must be object or d1, got: $OFS_METADATA_MODE"
    ;;
esac

config="$OFS_CASE_ROOT/client/config.json"
observed_config="$OFS_CASE_ROOT/observed-client/config.json"
main_replica="$OFS_CASE_ROOT/main"
experiment_replica="$OFS_CASE_ROOT/experiment"
main_cold="$OFS_CASE_ROOT/main-cold"
experiment_cold="$OFS_CASE_ROOT/experiment-cold"
rewind_replica="$OFS_CASE_ROOT/rewind"
empty_replica="$OFS_CASE_ROOT/empty"
new_experiment="$OFS_CASE_ROOT/new-experiment"
rewind_cold="$OFS_CASE_ROOT/rewind-cold"
large_replica="$OFS_CASE_ROOT/large"
large_parent="$OFS_CASE_ROOT/large-parent"
state_root="$OFS_CASE_ROOT/state"
main_state="$state_root/main.json"
experiment_state="$state_root/experiment.json"

mkdir -p "$(dirname "$config")" "$(dirname "$observed_config")" \
  "$main_replica" "$experiment_replica" \
  "$main_cold" "$experiment_cold" "$rewind_replica" "$new_experiment" \
  "$empty_replica" "$rewind_cold" "$large_replica" "$large_parent" \
  "$state_root"

volume_options=(--model managed --enable branch --storage "$OFS_STORAGE_URL")

printf '%s\n' 'acceptance: create a branching volume with the default main branch'
OFS_CONFIG="$config" "$OFS_BIN" volume create workspace "${volume_options[@]}"
branches=$(OFS_CONFIG="$config" "$OFS_BIN" branch workspace list --json)
python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value["default_branch"] == "main"
assert [branch["name"] for branch in value["branches"]] == ["main"]
' <<<"$branches" || fail 'new branching volume did not expose only default branch main'

if [[ "$OFS_METADATA_MODE" == object ]]; then
  OFS_CONFIG="$observed_config" "$OFS_BIN" volume create observed-workspace \
    --model managed --storage "$OFS_STORAGE_URL" >/dev/null
  observed_branches=$(OFS_CONFIG="$observed_config" "$OFS_BIN" branch observed-workspace list --json)
  python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value["default_branch"] == "main"
assert [branch["name"] for branch in value["branches"]] == ["main"]
' <<<"$observed_branches" || \
    fail 'a new client did not honor the remote branch/v1 format extension'
fi

printf '%s\n' 'anchor state' >"$main_replica/shared.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$main_replica" --state "$main_state"
main_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$main_state" --json)
anchor_sequence=$(json_field "$main_status" common_sequence)
[[ "$(json_field "$main_status" branch_name)" == main ]] || fail 'default sync did not bind main'
main_branch=$(OFS_CONFIG="$config" "$OFS_BIN" branch workspace show main --json)
[[ "$(json_field "$main_branch" name)" == main ]] || fail 'branch show returned another branch'
[[ "$(json_field "$main_branch" sequence)" == "$anchor_sequence" ]] || \
  fail 'branch show did not report the durable Sync position'

printf '%s\n' 'acceptance: fork current state and publish independently'
OFS_CONFIG="$config" "$OFS_BIN" branch workspace create experiment
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$experiment_replica" \
  --branch experiment --state "$experiment_state"
cmp "$main_replica/shared.txt" "$experiment_replica/shared.txt" || fail 'fork did not retain source state'

printf '%s\n' 'main state' >"$main_replica/shared.txt"
printf '%s\n' 'experiment state' >"$experiment_replica/shared.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$main_replica" --state "$main_state"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$experiment_replica" \
  --branch experiment --state "$experiment_state"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$main_cold" \
  --state "$state_root/main-cold.json"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$experiment_cold" \
  --branch experiment --state "$state_root/experiment-cold.json"
grep -Fxq 'main state' "$main_cold/shared.txt" || fail 'main observed another branch publication'
grep -Fxq 'experiment state' "$experiment_cold/shared.txt" || fail 'experiment observed main publication'

printf '%s\n' 'acceptance: fork an old published position after a long branch history'
for generation in $(seq 1 66); do
  printf 'main generation %s\n' "$generation" >"$main_replica/history.txt"
  OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$main_replica" \
    --state "$main_state" >/dev/null
done
OFS_CONFIG="$config" "$OFS_BIN" branch workspace create rewind --from main --at "$anchor_sequence"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$rewind_replica" \
  --branch rewind --state "$state_root/rewind.json"
grep -Fxq 'anchor state' "$rewind_replica/shared.txt" || fail 'historical fork lost its source content'
[[ ! -e "$rewind_replica/history.txt" ]] || fail 'historical fork included later content'

OFS_CONFIG="$config" "$OFS_BIN" branch workspace create empty --from main --at 0
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$empty_replica" \
  --branch empty --state "$state_root/empty.json"
[[ -z "$(find "$empty_replica" -mindepth 1 -print -quit)" ]] || \
  fail 'fork at change zero was not empty'

printf '%s\n' 'acceptance: reject stale replica state after delete and name reuse'
old_experiment_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$experiment_state" --json)
old_experiment_tree=$(tree_digest "$experiment_replica")
old_experiment_id=$(json_field "$old_experiment_status" branch_id)
OFS_CONFIG="$config" "$OFS_BIN" branch workspace delete experiment
OFS_CONFIG="$config" "$OFS_BIN" branch workspace create experiment --from main
if OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$experiment_replica" \
  --branch experiment --state "$experiment_state" 2>"$OFS_CASE_ROOT/stale.err"; then
  fail 'old replica attached to a recreated branch name'
fi
grep -Fq 'branch incarnation' "$OFS_CASE_ROOT/stale.err" || fail 'stale replica rejection was not actionable'
[[ "$(tree_digest "$experiment_replica")" == "$old_experiment_tree" ]] || \
  fail 'stale replica rejection changed user files'
[[ "$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$experiment_state" --json)" == \
  "$old_experiment_status" ]] || fail 'stale replica rejection changed durable replica status'

OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$new_experiment" --branch experiment \
  --state "$state_root/new-experiment.json"
new_status=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_root/new-experiment.json" --json)
[[ "$(json_field "$new_status" branch_id)" != "$old_experiment_id" ]] || \
  fail 'recreated branch reused its deleted incarnation'
grep -Fxq 'main state' "$new_experiment/shared.txt" || fail 'recreated branch did not fork current main'

printf '%s\n' 'acceptance: collection preserves every active and historical branch root'
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  if AWS_SECRET_ACCESS_KEY=invalid OFS_CONFIG="$config" \
    "$OFS_BIN" volume gc workspace >/dev/null 2>&1; then
    fail 'collection unexpectedly completed with unavailable data storage'
  fi
  if OFS_CONFIG="$config" "$OFS_BIN" volume gc workspace >/dev/null 2>&1; then
    fail 'a new collector replaced an interrupted collection'
  fi
  collection=$(OFS_CONFIG="$config" "$OFS_BIN" volume gc workspace --resume)
else
  collection=$(OFS_CONFIG="$config" "$OFS_BIN" volume gc workspace)
fi
grep -Eq 'deleted=[1-9][0-9]*' <<<"$collection" || \
  fail 'branch reachability collection removed no orphaned segment'
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$rewind_cold" --branch rewind \
  --state "$state_root/rewind-cold.json"
diff -ru "$rewind_replica" "$rewind_cold" || fail 'collection removed historical branch content'

printf '%s\n' 'acceptance: branch status is complete and does not expose secrets'
status_json=$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_root/rewind-cold.json" --json)
[[ "$(json_field "$status_json" branch_name)" == rewind ]] || fail 'status omitted branch name'
grep -Eq '"branch_id"[[:space:]]*:[[:space:]]*"[0-9a-f]{32}"' <<<"$status_json" || \
  fail 'status omitted the complete branch identity'
for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN OFS_D1_TOKEN; do
  secret=${!name:-}
  if [[ -n "$secret" ]] && grep -Fq -- "$secret" <<<"$status_json"; then
    fail "status leaked credential from $name"
  fi
done

printf '%s\n' 'scale regression: publish a large namespace change and retain its parent'
OFS_CONFIG="$config" "$OFS_BIN" branch workspace create large --from main --at 0
printf '%s\n' seed >"$large_replica/seed.txt"
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$large_replica" \
  --branch large --state "$state_root/large.json" >/dev/null
large_parent_sequence=$(json_field \
  "$(OFS_CONFIG="$config" "$OFS_BIN" status --state "$state_root/large.json" --json)" \
  common_sequence)
bulk_suffix=$(printf '%0180d' 0 | tr '0' x)
for number in $(seq -w 1 2200); do
  : >"$large_replica/bulk-$number-$bulk_suffix"
done
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$large_replica" \
  --branch large --state "$state_root/large.json" >/dev/null
OFS_CONFIG="$config" "$OFS_BIN" branch workspace create large-parent \
  --from large --at "$large_parent_sequence" >/dev/null
OFS_CONFIG="$config" "$OFS_BIN" sync workspace "$large_parent" \
  --branch large-parent --state "$state_root/large-parent.json" >/dev/null
grep -Fxq seed "$large_parent/seed.txt" || fail 'large publication parent lost its seed state'
if find "$large_parent" -maxdepth 1 -name 'bulk-*' -print -quit | grep -q .; then
  fail 'large publication was copied into its retained parent'
fi

printf 'managed-branch acceptance passed (%s metadata)\n' "$OFS_METADATA_MODE"
