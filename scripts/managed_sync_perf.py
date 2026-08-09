#!/usr/bin/env python3
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

"""Run one Managed Sync A/B comparison and retain one canonical report."""

import argparse
import datetime
import os
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tests/performance/managed-sync/run.sh"
DERIVED_REPORTS = (
    "context.tsv",
    "inputs.tsv",
    "samples.tsv",
)


def source(environment: dict[str, str], role: str, value: str | None) -> None:
    if value is None:
        return
    path = pathlib.Path(value).expanduser()
    key = f"OFS_PERF_{role.upper()}"
    if path.is_file():
        if not os.access(path, os.X_OK):
            raise SystemExit(f"{role} binary is not executable: {path}")
        environment[f"{key}_BIN"] = str(path.resolve())
        return
    subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", f"{value}^{{commit}}"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    environment[key] = value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="compare Managed Sync request, byte, object, and latency behavior"
    )
    parser.add_argument("output", nargs="?", type=pathlib.Path)
    parser.add_argument("--baseline")
    parser.add_argument("--candidate")
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--profile", choices=("standard", "agent-home"), default="standard")
    arguments = parser.parse_args()
    if arguments.rounds < 1:
        parser.error("--rounds must be greater than zero")

    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    output = (arguments.output or ROOT / f".local/performance/managed-sync-ab-{stamp}").resolve()
    if output.exists():
        parser.error(f"output path already exists: {output}")

    environment = os.environ.copy()
    environment["OFS_PERF_ROUNDS"] = str(arguments.rounds)
    environment["OFS_PERF_PROFILE"] = arguments.profile
    source(environment, "baseline", arguments.baseline)
    source(environment, "candidate", arguments.candidate)
    subprocess.run(["bash", str(RUNNER), str(output)], cwd=ROOT, env=environment, check=True)

    for name in DERIVED_REPORTS:
        (output / name).unlink(missing_ok=True)
    print(f"canonical evidence: {output / 'results.json'}")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from error
