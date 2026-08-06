#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0.

# Public black-box workload: 1000 sanitized agent files, four replicas, one
# incremental publication per update, lagged catch-up, conflict, and cold rebuild.

set -euo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
generations=100
output=

while (($#)); do
  case $1 in
    --generations) generations=$2; shift 2 ;;
    --output) output=$2; shift 2 ;;
    -h|--help)
      printf 'Usage: stress.sh [--generations N] [--output NEW-DIRECTORY]\n'
      exit 0
      ;;
    *) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done
[[ $generations =~ ^[1-9][0-9]*$ ]] || { printf 'generations must be positive\n' >&2; exit 2; }

files=1000
agents=4
changes=4
runtime=${CONTAINER_RUNTIME:-podman}
ofs_bin=${OFS_BIN:-$workspace/target/release/ofs}
implementation=${OFS_IMPLEMENTATION:-$(git -C "$workspace" rev-parse --short=12 HEAD)}
if [[ -z $output ]]; then
  output="$workspace/.local/research/managed-sync-stress/runs/native-${generations}g-$(date -u +%Y%m%d%H%M%S)"
fi
[[ ! -e $output ]] || { printf 'output already exists: %s\n' "$output" >&2; exit 2; }
for command in "$runtime" curl diff find python3 sha256sum sort stat /usr/bin/time; do
  command -v "$command" >/dev/null
done
test -x "$ofs_bin"
mkdir -p "$output"
output=$(cd "$output" && pwd)

container="ofs-stress-${PPID}-$$"
tracer="ofs-stress-trace-${PPID}-$$"
trace_pid=
passed=false
cleanup() {
  status=$?
  "$runtime" rm -f "$tracer" >/dev/null 2>&1 || true
  if [[ -n $trace_pid ]]; then wait "$trace_pid" 2>/dev/null || true; fi
  "$runtime" rm -f "$container" >>"$output/minio.log" 2>&1 || status=1
  if ! $passed; then
    printf 'Managed Sync stress evidence retained at %s\n' "$output" >&2
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$runtime" run -d --rm --name "$container" -p 127.0.0.1::9000 \
  -e MINIO_ROOT_USER=ofs-stress -e MINIO_ROOT_PASSWORD=ofs-stress-password \
  -e MINIO_PROMETHEUS_AUTH_TYPE=public \
  quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z server /data \
  >"$output/minio.log" 2>&1
port=$("$runtime" port "$container" 9000/tcp | sed -n 's/.*://p')
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$port/minio/health/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$port/minio/health/ready" >/dev/null

mc_image=quay.io/minio/mc:RELEASE.2024-09-16T17-43-14Z
mc_alias="mc alias set stress http://127.0.0.1:$port ofs-stress ofs-stress-password >/dev/null"
"$runtime" run --rm --network host --entrypoint /bin/sh "$mc_image" -c \
  "$mc_alias && mc mb stress/managed-sync >/dev/null"
"$runtime" run --rm --name "$tracer" --network host --entrypoint /bin/sh "$mc_image" -c \
  "$mc_alias && mc admin trace --json stress" >"$output/minio-trace.jsonl" 2>>"$output/minio.log" &
trace_pid=$!
sleep 0.3

metrics_url="http://127.0.0.1:$port/minio/v2/metrics/cluster"
curl -fsS "$metrics_url" >"$output/metrics-before.prom"
export OFS_STORAGE_URL="s3://?bucket=managed-sync&root=stress&endpoint=http://127.0.0.1:$port&region=us-east-1&access_key_id=ofs-stress&secret_access_key=ofs-stress-password"
catalog="$output/volumes.json"
volume='agent-skills-stress'
trees="$output/trees"
states="$output/states"
mkdir "$trees" "$states"

tree_digest() {
  local root=$1
  (
    cd "$root"
    find . -mindepth 1 -type d -printf 'd\t%P\n' | sort
    while IFS= read -r -d '' path; do
      printf 'f\t%s\t%s\t%s\n' "$path" "$(stat -c %s "$path")" \
        "$(sha256sum "$path" | cut -d' ' -f1)"
    done < <(find . -type f -printf '%P\0' | sort -z)
  ) | sha256sum | cut -d' ' -f1
}

timed_sync() {
  local label=$1 tree=$2 state=$3 timing="$output/.timing"
  shift 3
  local rc=0
  /usr/bin/time -q -f $'%e\t%U\t%S\t%M' -o "$timing" \
    "$ofs_bin" --config "$catalog" sync "$volume" "$tree" --state "$state" "$@" \
    >>"$output/sync.log" 2>&1 || rc=$?
  IFS=$'\t' read -r wall user system rss <"$timing"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$wall" "$user" "$system" "$rss" "$rc"
  return "$rc"
}

assert_clean() {
  local tree=$1 state=$2 generation=$3 status_file=$4
  "$ofs_bin" --config "$catalog" status "$tree" --state "$state" --json >"$status_file"
  python3 - "$status_file" "$generation" <<'PY'
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
g = int(sys.argv[2])
assert s["local"] == "clean" and s["base"]["generation"] == g
assert s["remote"] == {"state": "at_base", "generation": g}
assert s["publication"] == s["materialize"] == "idle" and s["conflicts"] == 0
PY
}

initial="$trees/agent-0"
mkdir -p "$initial/.agents/skills" "$initial/.bub/tapes" "$initial/.codex/sessions"
for index in $(seq 0 $((files - 1))); do
  owner=$((index % agents))
  path="$initial/.agents/skills/agent-$(printf '%02d' "$owner")/skill-$(printf '%05d' "$index")/SKILL.md"
  mkdir -p "$(dirname "$path")"
  printf '# Skill %05d\n\nSynthetic content owned by agent %02d.\n' "$index" "$owner" >"$path"
done
for agent in $(seq 0 $((agents - 1))); do
  printf 'tape %02d\n' "$agent" >"$initial/.bub/tapes/agent-$(printf '%02d' "$agent").jsonl"
  printf 'session %02d\n' "$agent" >"$initial/.codex/sessions/agent-$(printf '%02d' "$agent").jsonl"
done

printf 'phase\twall_s\tuser_s\tsys_s\tmax_rss_kib\texit\n' >"$output/phases.tsv"
printf 'update\tagent\twall_s\tuser_s\tsys_s\tmax_rss_kib\texit\n' >"$output/generations.tsv"
printf 'lag_generations\ttarget_generation\twall_s\tmax_rss_kib\n' >"$output/catchup.tsv"
"$ofs_bin" --config "$catalog" volume create "$volume" --model managed --storage "$OFS_STORAGE_URL" \
  >"$output/volume-create.log" 2>&1
timed_sync initial-publication "$initial" "$states/agent-0" >>"$output/phases.tsv"

for agent in 1 2 3; do
  mkdir "$trees/agent-$agent"
  timed_sync "agent-$agent-cold" "$trees/agent-$agent" "$states/agent-$agent" >>"$output/phases.tsv"
done
for lag in 10 50 100; do
  mkdir "$trees/lag-$lag"
  timed_sync "lag-$lag-baseline" "$trees/lag-$lag" "$states/lag-$lag" >>"$output/phases.tsv"
done

points=($((generations / 10)) $((generations / 2)) "$generations")
for update in $(seq 1 "$generations"); do
  agent=$(((update - 1) % agents))
  tree="$trees/agent-$agent"
  for change in $(seq 0 $((changes - 1))); do
    slot=$(((update * changes + change) % (files / agents)))
    index=$((agent + slot * agents))
    path="$tree/.agents/skills/agent-$(printf '%02d' "$agent")/skill-$(printf '%05d' "$index")/SKILL.md"
    printf '# Skill %05d\n\nAgent %02d update %06d change %02d.\n' "$index" "$agent" "$update" "$change" >"$path"
  done
  if ((update % 25 == 0)); then
    path="$tree/.agents/skills/agent-$(printf '%02d' "$agent")/runtime/update-$(printf '%06d' "$update").md"
    mkdir -p "$(dirname "$path")"
    printf 'agent %02d update %06d\n' "$agent" "$update" >"$path"
  fi
  if ((update > 100 && update % 100 == 0)); then
    old=$((update - 100))
    rm -f "$tree/.agents/skills/agent-$(printf '%02d' "$agent")/runtime/update-$(printf '%06d' "$old").md"
  fi
  timing=$(timed_sync "update-$update" "$tree" "$states/agent-$agent")
  IFS=$'\t' read -r _ wall user system rss rc <<<"$timing"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$update" "$agent" "$wall" "$user" "$system" "$rss" "$rc" \
    >>"$output/generations.tsv"

  for point_index in 0 1 2; do
    if ((update == points[point_index])); then
      lag=${points[point_index]}
      label=$((point_index == 0 ? 10 : point_index == 1 ? 50 : 100))
      timing=$(timed_sync "lag-$label-catch-up" "$trees/lag-$label" "$states/lag-$label")
      IFS=$'\t' read -r _ catch_wall _ _ catch_rss _ <<<"$timing"
      printf '%s\t%s\t%s\t%s\n' "$lag" "$((update + 1))" "$catch_wall" "$catch_rss" >>"$output/catchup.tsv"
    fi
  done
done

steady=$((generations + 1))
for agent in 0 1 2 3; do
  timed_sync "agent-$agent-final" "$trees/agent-$agent" "$states/agent-$agent" >>"$output/phases.tsv"
  assert_clean "$trees/agent-$agent" "$states/agent-$agent" "$steady" "$output/agent-$agent.status.json"
done
expected=$(tree_digest "$trees/agent-0")
for agent in 1 2 3; do test "$(tree_digest "$trees/agent-$agent")" = "$expected"; done

conflict=.agents/skills/agent-00/skill-00000/SKILL.md
printf '# Conflict\n\nAgent 0 candidate.\n' >"$trees/agent-0/$conflict"
printf '# Conflict\n\nAgent 1 candidate.\n' >"$trees/agent-1/$conflict"
timed_sync conflict-winner "$trees/agent-0" "$states/agent-0" >>"$output/phases.tsv"
if timed_sync conflict-observer "$trees/agent-1" "$states/agent-1" >>"$output/phases.tsv"; then
  printf 'same-file conflict unexpectedly published\n' >&2
  exit 1
fi
grep -Fqx 'Agent 1 candidate.' "$trees/agent-1/$conflict"
timed_sync conflict-resolution "$trees/agent-1" "$states/agent-1" --resolve "$conflict" >>"$output/phases.tsv"
final=$((steady + 2))
assert_clean "$trees/agent-1" "$states/agent-1" "$final" "$output/resolution.status.json"

mkdir "$trees/cold-final"
timed_sync cold-rebuild "$trees/cold-final" "$states/cold-final" >>"$output/phases.tsv"
assert_clean "$trees/cold-final" "$states/cold-final" "$final" "$output/cold.status.json"
final_digest=$(tree_digest "$trees/agent-1")
test "$(tree_digest "$trees/cold-final")" = "$final_digest"

curl -fsS "$metrics_url" >"$output/metrics-after.prom"
"$runtime" rm -f "$tracer" >/dev/null 2>&1 || true
wait "$trace_pid" 2>/dev/null || true
trace_pid=
"$runtime" run --rm --network host --entrypoint /bin/sh "$mc_image" -c \
  "$mc_alias && mc ls --recursive --json stress/managed-sync/stress" \
  >"$output/minio-objects.jsonl" 2>>"$output/minio.log"

python3 - "$output" <<'PY'
import collections, json, pathlib, re, sys
root = pathlib.Path(sys.argv[1])

requests = collections.Counter()
for line in (root / "minio-trace.jsonl").open(encoding="utf-8"):
    try: record = json.loads(line)
    except json.JSONDecodeError: continue
    if record.get("type") == "S3": requests[record.get("api", "unknown")] += 1
with (root / "requests.tsv").open("w", encoding="utf-8") as out:
    out.write("api\trequests\n")
    for api, count in sorted(requests.items()): out.write(f"{api}\t{count}\n")

inventory = collections.Counter()
for line in (root / "minio-objects.jsonl").open(encoding="utf-8"):
    try: record = json.loads(line)
    except json.JSONDecodeError: continue
    key, size = record.get("key", ""), int(record.get("size", 0))
    if "metadata/commits/" in key: kind = "change_commits"
    elif "data/sha256/" in key: kind = "immutable_data"
    else: kind = "authority"
    inventory[(kind, "objects")] += 1
    inventory[(kind, "bytes")] += size
    inventory[(kind, "max")] = max(inventory[(kind, "max")], size)
with (root / "inventory.tsv").open("w", encoding="utf-8") as out:
    out.write("kind\tobjects\tbytes\tmax_object_bytes\n")
    for kind in ("change_commits", "immutable_data", "authority"):
        out.write(f"{kind}\t{inventory[kind, 'objects']}\t{inventory[kind, 'bytes']}\t{inventory[kind, 'max']}\n")

def metric(path, name):
    text = path.read_text(encoding="utf-8")
    return sum(float(value) for value in re.findall(rf"^{name}(?:\{{[^}}]*\}})? ([0-9.e+-]+)$", text, re.M))
before, after = root / "metrics-before.prom", root / "metrics-after.prom"
received = int(metric(after, "minio_s3_traffic_received_bytes") - metric(before, "minio_s3_traffic_received_bytes"))
sent = int(metric(after, "minio_s3_traffic_sent_bytes") - metric(before, "minio_s3_traffic_sent_bytes"))
(root / "traffic.tsv").write_text(f"direction\tbytes\nreceived\t{received}\nsent\t{sent}\n", encoding="utf-8")
PY

publication_summary=$(awk -F '\t' 'NR > 1 {sum += $3; if ($3 > max) max = $3; if ($6 > rss) rss = $6; n++}
  END {printf "%d %.6f %.6f %.6f %d", n, sum, sum/n, max, rss}' "$output/generations.tsv")
read -r publication_count total_wall mean_wall max_wall publication_rss <<<"$publication_summary"
request_total=$(awk -F '\t' 'NR > 1 {sum += $2} END {print sum + 0}' "$output/requests.tsv")
{
  printf 'managed-sync-minio-stress\nimplementation=%s\n' "$implementation"
  printf 'initial_files=%s\nupdate_generations=%s\nagents=%s\nchanges_per_generation=%s\n' \
    "$files" "$generations" "$agents" "$changes"
  printf 'final_generation=%s\nfinal_tree_digest=%s\n' "$final" "$final_digest"
  printf 'publication_count=%s\npublication_total_wall_s=%s\npublication_mean_wall_s=%s\n' \
    "$publication_count" "$total_wall" "$mean_wall"
  printf 'publication_max_wall_s=%s\npublication_max_rss_kib=%s\nminio_s3_requests=%s\n' \
    "$max_wall" "$publication_rss" "$request_total"
  awk -F '\t' '$1 == "initial-publication" {printf "initial_publication_wall_s=%s\ninitial_publication_max_rss_kib=%s\n", $2,$5}
    $1 == "cold-rebuild" {printf "cold_rebuild_wall_s=%s\ncold_rebuild_max_rss_kib=%s\n", $2,$5}' "$output/phases.tsv"
  awk -F '\t' 'NR > 1 {printf "catchup_%s_generations_wall_s=%s\ncatchup_%s_generations_max_rss_kib=%s\n", $1,$3,$1,$4}' "$output/catchup.tsv"
  awk -F '\t' 'NR > 1 {printf "%s_objects=%s\n%s_bytes=%s\n%s_max_object_bytes=%s\n", $1,$2,$1,$3,$1,$4}' "$output/inventory.tsv"
  awk -F '\t' '$1 == "change_commits" && $2 > 1 {printf "incremental_commit_mean_bytes=%.2f\n", ($3-$4)/($2-1)}' "$output/inventory.tsv"
  awk -F '\t' 'NR > 1 {printf "minio_%s_bytes=%s\n", $1,$2}' "$output/traffic.tsv"
  printf 'exact_convergence=true\nconflict_retained=true\nresolution_published=true\ncold_rebuild_match=true\nresult=passed\n'
} >"$output/evidence.txt"

rm -rf "$trees" "$states"
rm -f "$output/minio-trace.jsonl" "$output/minio-objects.jsonl" \
  "$output/metrics-before.prom" "$output/metrics-after.prom"
passed=true
printf 'Managed Sync MinIO stress passed: %s\n' "$output"
