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

usage() {
  cat <<'EOF'
Usage: scripts/managed-sync-compare.sh --baseline BINARY_OR_REF [OPTIONS]

Run the same Managed Sync lifecycle through a baseline and the current binary.

Options:
  --baseline PATH_OR_REF  Required executable or git branch/commit.
  --candidate PATH_OR_REF Executable or git branch/commit; defaults to a fresh
                          release build of the current working tree.
  --output DIRECTORY      New evidence directory.
  --rounds N              Lifecycle generations (default: 12).
  -h, --help              Show this help.
EOF
}

workspace=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
baseline=
candidate=
output=
rounds=12
while (($#)); do
  case $1 in
    --baseline|--candidate|--output|--rounds)
      (($# >= 2)) || { printf 'missing value for %s\n' "$1" >&2; exit 2; }
      name=${1#--}
      printf -v "$name" '%s' "$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unexpected argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n $baseline ]] || { printf '%s\n' '--baseline is required' >&2; exit 2; }
[[ $rounds =~ ^[1-9][0-9]*$ ]] || { printf '%s\n' '--rounds must be greater than zero' >&2; exit 2; }

declare -a settings=("OFS_PERF_ROUNDS=$rounds")
select_source() {
  local role=$1 source=$2 path
  if [[ -f $source ]]; then
    [[ -x $source ]] || { printf '%s binary is not executable: %s\n' "$role" "$source" >&2; exit 2; }
    path=$(cd "$(dirname "$source")" && pwd)/$(basename "$source")
    settings+=("OFS_PERF_${role^^}_BIN=$path")
  else
    git -C "$workspace" rev-parse --verify --quiet "$source^{commit}" >/dev/null || {
      printf '%s is neither an executable nor a git ref: %s\n' "$role" "$source" >&2
      exit 2
    }
    settings+=("OFS_PERF_${role^^}=$source")
  fi
}

select_source baseline "$baseline"
if [[ -n $candidate ]]; then
  select_source candidate "$candidate"
else
  cargo build --manifest-path "$workspace/Cargo.toml" --release --locked --bin ofs
  target_directory=$(cargo metadata --manifest-path "$workspace/Cargo.toml" \
    --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  select_source candidate "$target_directory/release/ofs"
fi

command=(bash "$workspace/tests/performance/managed-sync/run.sh")
if [[ -n $output ]]; then
  command+=("$output")
fi
env "${settings[@]}" "${command[@]}"
