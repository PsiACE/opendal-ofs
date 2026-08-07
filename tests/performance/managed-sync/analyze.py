#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

"""Summarize release A/B evidence and enforce user-visible performance gates."""

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def read_tsv(path: Path) -> list[list[str]]:
    return [line.rstrip("\n").split("\t") for line in path.open(encoding="utf-8")]


def percentile(values: list[int], quantile: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def load_metrics(path: Path):
    samples = []
    intervals = defaultdict(list)
    for release, run, metric, sample, value, started, ended in read_tsv(path):
        record = {
            "release": release,
            "run": run,
            "metric": metric,
            "sample": sample,
            "value_ms": int(value),
            "start_ns": int(started),
            "end_ns": int(ended),
        }
        samples.append(record)
        if metric != "lifecycle":
            intervals[run].append(record)
    return samples, intervals


def summarize(samples):
    grouped = defaultdict(list)
    for sample in samples:
        grouped[(sample["release"], sample["metric"])].append(sample["value_ms"])
    return {
        f"{release}.{metric}": {
            "count": len(values),
            "median_ms": statistics.median(values),
            "p95_ms": percentile(values, 0.95),
        }
        for (release, metric), values in sorted(grouped.items())
    }


def request_phase(start_ns: int, intervals) -> str:
    for interval in intervals:
        if interval["start_ns"] <= start_ns <= interval["end_ns"]:
            return interval["metric"]
    return "setup_or_lifecycle"


def load_requests(path: Path, intervals):
    distribution = defaultdict(lambda: {"count": 0, "bytes_in": 0, "bytes_out": 0})
    uploaded = defaultdict(lambda: {"request_bytes": 0, "data_put_bytes": 0})
    observed_runs = set()
    noop_data_puts = []
    for line in path.open(encoding="utf-8"):
        request = json.loads(line)
        run = next((name for name in intervals if f"/ab/{name}/" in request["path"]), None)
        if run is None:
            continue
        observed_runs.add(run)
        phase = request_phase(request["start_ns"], intervals[run])
        object_class = "data" if "/data/" in request["path"] else "metadata"
        key = (run, phase, request["method"], object_class)
        distribution[key]["count"] += 1
        distribution[key]["bytes_in"] += request["bytes_in"]
        distribution[key]["bytes_out"] += request["bytes_out"]
        uploaded[run]["request_bytes"] += request["bytes_in"]
        if request["method"] == "PUT" and object_class == "data":
            uploaded[run]["data_put_bytes"] += request["bytes_in"]
            if phase == "noop":
                noop_data_puts.append(request)
    rows = [
        {
            "run": key[0],
            "phase": key[1],
            "method": key[2],
            "object_class": key[3],
            **values,
        }
        for key, values in sorted(distribution.items())
    ]
    return rows, dict(uploaded), observed_runs, noop_data_puts


def load_inputs(path: Path):
    values = defaultdict(dict)
    for release, run, key, value in read_tsv(path):
        values[run]["release"] = release
        values[run][key] = int(value) if value.isdigit() else value
    return dict(values)


def relative_gate(name: str, baseline: float, candidate: float, limit: float):
    delta = candidate / baseline - 1
    return {
        "name": name,
        "baseline": baseline,
        "candidate": candidate,
        "delta_ratio": delta,
        "limit_ratio": limit,
        "passed": delta <= limit,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    arguments = parser.parse_args()
    directory = arguments.directory
    samples, intervals = load_metrics(directory / "samples.tsv")
    statistics_by_metric = summarize(samples)
    requests, uploaded, observed_runs, noop_data_puts = load_requests(
        directory / "requests.jsonl", intervals
    )
    inputs = load_inputs(directory / "inputs.tsv")
    for run, values in uploaded.items():
        inputs[run].update(values)
    context = {key: value for key, value in read_tsv(directory / "context.tsv")}
    gates = [
        relative_gate(
            "lifecycle_median",
            statistics_by_metric["baseline.lifecycle"]["median_ms"],
            statistics_by_metric["candidate.lifecycle"]["median_ms"],
            0.10,
        ),
        relative_gate(
            "publication_p95",
            statistics_by_metric["baseline.publication"]["p95_ms"],
            statistics_by_metric["candidate.publication"]["p95_ms"],
            0.15,
        ),
        relative_gate(
            "catchup_p95",
            statistics_by_metric["baseline.catchup"]["p95_ms"],
            statistics_by_metric["candidate.catchup"]["p95_ms"],
            0.15,
        ),
        {
            "name": "noop_requests_observed",
            "passed": {s["run"] for s in samples if s["metric"] == "noop"} <= observed_runs,
        },
        {
            "name": "noop_has_no_data_put",
            "count": len(noop_data_puts),
            "passed": not noop_data_puts,
        },
        {
            "name": "object_inventory_observed",
            "passed": all(
                isinstance(values.get("stored_bytes"), int)
                and isinstance(values.get("stored_objects"), int)
                for values in inputs.values()
            ),
        },
    ]
    verdict = "pass" if all(gate["passed"] for gate in gates) else "fail"
    report = {
        "format": "ofs-managed-sync-performance",
        "version": 1,
        "verdict": verdict,
        "context": context,
        "order": ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"],
        "inputs_and_storage": inputs,
        "samples": samples,
        "statistics": statistics_by_metric,
        "request_distribution": requests,
        "gates": gates,
    }
    (directory / "results.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (directory / "requests.tsv").open("w", encoding="utf-8") as output:
        output.write("run\tphase\tmethod\tobject_class\tcount\tbytes_in\tbytes_out\n")
        for row in requests:
            output.write("\t".join(str(row[key]) for key in row) + "\n")
    print(json.dumps({"verdict": verdict, "statistics": statistics_by_metric, "gates": gates}, indent=2))
    raise SystemExit(verdict != "pass")


if __name__ == "__main__":
    main()
