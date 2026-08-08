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
from urllib.parse import unquote


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


def object_class(path: str) -> str:
    decoded = "/" + unquote(path).lstrip("/")
    if "/.ofs/managed/indexes/data-pack/v1/packs/sha256/" in decoded:
        return "pack_data"
    if "/.ofs/managed/indexes/data-pack/v1/" in decoded:
        return "secondary_index"
    if "/.ofs/managed/data/v1/loose/sha256/" in decoded or "/data/sha256/" in decoded:
        return "loose_data"
    if decoded.endswith("/.ofs/managed/volume.json") or decoded.endswith("/metadata/format"):
        return "format"
    if "/.ofs/managed/metadata/v1/" in decoded or "/metadata/" in decoded:
        return "metadata"
    return "control"


def load_requests(path: Path, intervals):
    distribution = defaultdict(lambda: {"count": 0, "bytes_in": 0, "bytes_out": 0})
    uploaded = defaultdict(lambda: {"request_bytes": 0, "data_put_bytes": 0})
    observed_runs = set()
    noop_data_puts = []
    for line in path.open(encoding="utf-8"):
        request = json.loads(line)
        decoded_path = unquote(request["path"])
        run = next((name for name in intervals if f"/ab/{name}" in decoded_path), None)
        if run is None:
            continue
        observed_runs.add(run)
        phase = request_phase(request["start_ns"], intervals[run])
        classification = object_class(decoded_path)
        is_range = request.get("range") is not None
        key = (run, phase, request["method"], request["status"], classification, is_range)
        distribution[key]["count"] += 1
        distribution[key]["bytes_in"] += request["bytes_in"]
        distribution[key]["bytes_out"] += request["bytes_out"]
        uploaded[run]["request_bytes"] += request["bytes_in"]
        if request["method"] == "PUT" and classification in {"loose_data", "pack_data"}:
            uploaded[run]["data_put_bytes"] += request["bytes_in"]
            if phase == "noop":
                noop_data_puts.append(request)
    rows = [
        {
            "run": key[0],
            "phase": key[1],
            "method": key[2],
            "status": key[3],
            "object_class": key[4],
            "range": key[5],
            **values,
        }
        for key, values in sorted(distribution.items())
    ]
    return rows, dict(uploaded), observed_runs, noop_data_puts


def load_object_inventory(directory: Path, inputs):
    inventories = []
    for run, values in sorted(inputs.items()):
        totals = defaultdict(lambda: {"objects": 0, "bytes": 0})
        path = directory / "runs" / run / "objects.jsonl"
        for line in path.open(encoding="utf-8"):
            record = json.loads(line)
            if record.get("status") != "success" or record.get("type") != "file":
                continue
            classification = object_class(record["key"])
            totals[classification]["objects"] += 1
            totals[classification]["bytes"] += int(record["size"])
        for classification, total in sorted(totals.items()):
            inventories.append(
                {
                    "run": run,
                    "release": values["release"],
                    "object_class": classification,
                    **total,
                }
            )
    return inventories


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
    object_inventory = load_object_inventory(directory, inputs)
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
        "object_inventory": object_inventory,
        "gates": gates,
    }
    (directory / "results.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (directory / "requests.tsv").open("w", encoding="utf-8") as output:
        output.write(
            "run\tphase\tmethod\tstatus\tobject_class\trange\tcount\tbytes_in\tbytes_out\n"
        )
        for row in requests:
            output.write("\t".join(str(row[key]) for key in row) + "\n")
    with (directory / "objects.tsv").open("w", encoding="utf-8") as output:
        output.write("run\trelease\tobject_class\tobjects\tbytes\n")
        for row in object_inventory:
            output.write("\t".join(str(row[key]) for key in row) + "\n")
    print(json.dumps({"verdict": verdict, "statistics": statistics_by_metric, "gates": gates}, indent=2))
    raise SystemExit(verdict != "pass")


if __name__ == "__main__":
    main()
