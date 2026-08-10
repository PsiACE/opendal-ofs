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
  (cd "$root" && find . -type f -exec b3sum {} + | LC_ALL=C sort | b3sum)
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

command -v b3sum >/dev/null || fail 'b3sum is required'
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

main_replica="$OFS_CASE_ROOT/main"
observed_replica="$OFS_CASE_ROOT/observed"
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
observed_state="$state_root/observed.json"
experiment_state="$state_root/experiment.json"

mkdir -p "$main_replica" "$observed_replica" "$experiment_replica" \
  "$main_cold" "$experiment_cold" "$rewind_replica" "$new_experiment" \
  "$empty_replica" "$rewind_cold" "$large_replica" "$large_parent" \
  "$state_root"

target_options=(--model managed --storage "$OFS_STORAGE_URL")
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  target_options+=(--metadata "$OFS_METADATA_URL")
fi
unset OFS_STORAGE_URL OFS_METADATA_URL

attach() {
  local replica=$1 state=$2
  shift 2
  "$OFS_BIN" sync "$replica" --state "$state" "${target_options[@]}" "$@"
}

branch_cmd() {
  "$OFS_BIN" branch --state "$main_state" "$@"
}

gc_cmd() {
  "$OFS_BIN" volume gc --state "$main_state" "$@"
}

printf '%s\n' 'acceptance: initialize a branching volume with the default main branch'
"$OFS_BIN" sync "$main_replica" --state "$main_state" --init --enable branch \
  "${target_options[@]}" >/dev/null
branches=$(branch_cmd list --json)
python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value["default_branch"] == "main"
assert [branch["name"] for branch in value["branches"]] == ["main"]
' <<<"$branches" || fail 'new branching volume did not expose only default branch main'

if [[ "$OFS_METADATA_MODE" == object ]]; then
  attach "$observed_replica" "$observed_state" >/dev/null
  observed_branches=$("$OFS_BIN" branch --state "$observed_state" list --json)
  python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value["default_branch"] == "main"
assert [branch["name"] for branch in value["branches"]] == ["main"]
' <<<"$observed_branches" || \
    fail 'a new client did not honor the remote branch/v1 format extension'
fi

printf '%s\n' 'anchor state' >"$main_replica/shared.txt"
"$OFS_BIN" sync "$main_replica" --state "$main_state"
main_status=$("$OFS_BIN" status --state "$main_state" --json)
anchor_sequence=$(json_field "$main_status" common_sequence)
[[ "$(json_field "$main_status" branch_name)" == main ]] || fail 'default sync did not bind main'
main_branch=$(branch_cmd show main --json)
[[ "$(json_field "$main_branch" name)" == main ]] || fail 'branch show returned another branch'
[[ "$(json_field "$main_branch" sequence)" == "$anchor_sequence" ]] || \
  fail 'branch show did not report the durable Sync position'

printf '%s\n' 'acceptance: fork current state and publish independently'
branch_cmd create experiment
attach "$experiment_replica" "$experiment_state" --branch experiment
cmp "$main_replica/shared.txt" "$experiment_replica/shared.txt" || fail 'fork did not retain source state'

printf '%s\n' 'main state' >"$main_replica/shared.txt"
printf '%s\n' 'experiment state' >"$experiment_replica/shared.txt"
"$OFS_BIN" sync "$main_replica" --state "$main_state"
"$OFS_BIN" sync "$experiment_replica" --state "$experiment_state"
attach "$main_cold" "$state_root/main-cold.json"
attach "$experiment_cold" "$state_root/experiment-cold.json" --branch experiment
grep -Fxq 'main state' "$main_cold/shared.txt" || fail 'main observed another branch publication'
grep -Fxq 'experiment state' "$experiment_cold/shared.txt" || fail 'experiment observed main publication'

printf '%s\n' 'acceptance: fork an old published position after a long branch history'
for generation in $(seq 1 66); do
  printf 'main generation %s\n' "$generation" >"$main_replica/history.txt"
  "$OFS_BIN" sync "$main_replica" --state "$main_state" >/dev/null
done
branch_cmd create rewind --from main --at "$anchor_sequence"
attach "$rewind_replica" "$state_root/rewind.json" --branch rewind
grep -Fxq 'anchor state' "$rewind_replica/shared.txt" || fail 'historical fork lost its source content'
[[ ! -e "$rewind_replica/history.txt" ]] || fail 'historical fork included later content'

branch_cmd create empty --from main --at 0
attach "$empty_replica" "$state_root/empty.json" --branch empty
[[ -z "$(find "$empty_replica" -mindepth 1 -print -quit)" ]] || \
  fail 'fork at change zero was not empty'

printf '%s\n' 'acceptance: reject stale replica state after delete and name reuse'
old_experiment_status=$("$OFS_BIN" status --state "$experiment_state" --json)
old_experiment_tree=$(tree_digest "$experiment_replica")
old_experiment_id=$(json_field "$old_experiment_status" branch_id)
branch_cmd delete experiment
branch_cmd create experiment --from main
if "$OFS_BIN" sync "$experiment_replica" --state "$experiment_state" \
  2>"$OFS_CASE_ROOT/stale.err"; then
  fail 'old replica attached to a recreated branch name'
fi
grep -Fq 'branch incarnation' "$OFS_CASE_ROOT/stale.err" || fail 'stale replica rejection was not actionable'
[[ "$(tree_digest "$experiment_replica")" == "$old_experiment_tree" ]] || \
  fail 'stale replica rejection changed user files'
[[ "$("$OFS_BIN" status --state "$experiment_state" --json)" == \
  "$old_experiment_status" ]] || fail 'stale replica rejection changed durable replica status'

attach "$new_experiment" "$state_root/new-experiment.json" --branch experiment
new_status=$("$OFS_BIN" status --state "$state_root/new-experiment.json" --json)
[[ "$(json_field "$new_status" branch_id)" != "$old_experiment_id" ]] || \
  fail 'recreated branch reused its deleted incarnation'
grep -Fxq 'main state' "$new_experiment/shared.txt" || fail 'recreated branch did not fork current main'

printf '%s\n' 'acceptance: collection preserves every active and historical branch root'
if [[ "$OFS_METADATA_MODE" == d1 ]]; then
  if AWS_SECRET_ACCESS_KEY=invalid gc_cmd >/dev/null 2>&1; then
    fail 'collection unexpectedly completed with unavailable data storage'
  fi
  if gc_cmd >/dev/null 2>&1; then
    fail 'a new collector replaced an interrupted collection'
  fi
  collection=$(gc_cmd --resume)
else
  collection=$(gc_cmd)
fi
grep -Eq 'deleted=[1-9][0-9]*' <<<"$collection" || \
  fail 'branch reachability collection removed no orphaned segment'
attach "$rewind_cold" "$state_root/rewind-cold.json" --branch rewind
diff -ru "$rewind_replica" "$rewind_cold" || fail 'collection removed historical branch content'

printf '%s\n' 'acceptance: branch status is complete and does not expose secrets'
status_json=$("$OFS_BIN" status --state "$state_root/rewind-cold.json" --json)
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
branch_cmd create large --from main --at 0
printf '%s\n' seed >"$large_replica/seed.txt"
attach "$large_replica" "$state_root/large.json" --branch large >/dev/null
large_parent_sequence=$(json_field \
  "$("$OFS_BIN" status --state "$state_root/large.json" --json)" \
  common_sequence)
bulk_suffix=$(printf '%0180d' 0 | tr '0' x)
for number in $(seq -w 1 2200); do
  : >"$large_replica/bulk-$number-$bulk_suffix"
done
"$OFS_BIN" sync "$large_replica" --state "$state_root/large.json" >/dev/null
branch_cmd create large-parent --from large --at "$large_parent_sequence" >/dev/null
attach "$large_parent" "$state_root/large-parent.json" --branch large-parent >/dev/null
grep -Fxq seed "$large_parent/seed.txt" || fail 'large publication parent lost its seed state'
if find "$large_parent" -maxdepth 1 -name 'bulk-*' -print -quit | grep -q .; then
  fail 'large publication was copied into its retained parent'
fi

printf 'managed-branch acceptance passed (%s metadata)\n' "$OFS_METADATA_MODE"
