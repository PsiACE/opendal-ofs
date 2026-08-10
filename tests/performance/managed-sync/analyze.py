#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

"""Summarize release A/B evidence and verify its behavioral contracts."""

import argparse
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
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
    for release, run, metric, _sample, value, started, ended in read_tsv(path):
        record = {
            "release": release,
            "run": run,
            "metric": metric,
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


def request_phase(event_ns: int, intervals) -> str:
    for interval in intervals:
        if interval["start_ns"] <= event_ns <= interval["end_ns"]:
            return interval["metric"]
    return "unattributed"


def audit_time_ns(value: str) -> int:
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(instant.timestamp() * 1_000_000_000)


def object_class(path: str) -> str:
    decoded = "/" + unquote(path).lstrip("/")
    if "/.ofs/managed/data/v1/segments/sha256/" in decoded:
        return "segment_data"
    if decoded.endswith("/.ofs/managed/superblock.json"):
        return "format"
    if "/.ofs/managed/metadata/v1/" in decoded or "/metadata/" in decoded:
        return "metadata"
    return "control"


def load_audit(path: Path, intervals, access_key: str):
    distribution = defaultdict(lambda: {"count": 0, "bytes_in": 0, "bytes_out": 0})
    noop_data_puts = 0
    for line in path.open(encoding="utf-8"):
        event = json.loads(line)
        if event.get("accessKey") != access_key:
            continue
        api = event["api"]
        decoded_path = unquote(event["requestPath"])
        run = next((name for name in intervals if f"/ab/{name}" in decoded_path), None)
        if run is None:
            continue
        phase = request_phase(audit_time_ns(event["time"]), intervals[run])
        classification = object_class(decoded_path)
        is_range = "Range" in event.get("requestHeader", {})
        key = (run, phase, api["name"], api["statusCode"], classification, is_range)
        distribution[key]["count"] += 1
        distribution[key]["bytes_in"] += api["rx"]
        distribution[key]["bytes_out"] += api["tx"]
        if api["name"] == "PutObject" and classification == "segment_data":
            if phase == "noop":
                noop_data_puts += 1
    rows = [
        {
            "run": key[0],
            "phase": key[1],
            "operation": key[2],
            "status": key[3],
            "object_class": key[4],
            "range": key[5],
            **values,
        }
        for key, values in sorted(distribution.items())
    ]
    return rows, noop_data_puts


def load_object_totals(directory: Path, inputs):
    result = {}
    for run in sorted(inputs):
        classes = defaultdict(lambda: {"objects": 0, "bytes": 0})
        path = directory / "runs" / run / "objects.jsonl"
        for line in path.open(encoding="utf-8"):
            record = json.loads(line)
            if record.get("status") != "success" or record.get("type") != "file":
                continue
            classification = object_class(record["key"])
            classes[classification]["objects"] += 1
            classes[classification]["bytes"] += int(record["size"])
        metadata = [classes[name] for name in ("format", "metadata")]
        data = classes["segment_data"]
        result[run] = {
            "metadata_objects": sum(value["objects"] for value in metadata),
            "metadata_bytes": sum(value["bytes"] for value in metadata),
            "data_objects": data["objects"],
            "data_bytes": data["bytes"],
            "total_objects": sum(value["objects"] for value in classes.values()),
            "total_bytes": sum(value["bytes"] for value in classes.values()),
        }
    return result


def load_inputs(path: Path):
    values = defaultdict(dict)
    for release, run, key, value in read_tsv(path):
        values[run]["release"] = release
        values[run][key] = int(value) if value.isdigit() else value
    return dict(values)


def logical_equality(directory: Path, inputs) -> bool:
    manifests = [
        (directory / "runs" / run / "logical-tree.json").read_bytes()
        for run in sorted(inputs)
    ]
    return bool(manifests) and all(value == manifests[0] for value in manifests[1:])


def comparison_summary(statistics_by_metric, requests, object_totals, inputs):
    product_phases = {"cold_restore", "incremental_catchup", "publication", "noop"}
    request_totals = defaultdict(lambda: {"count": 0, "request_bytes": 0, "response_bytes": 0})
    range_gets = defaultdict(int)
    segment_gets = defaultdict(lambda: defaultdict(int))
    for row in requests:
        if row["phase"] not in product_phases:
            continue
        total = request_totals[row["run"]]
        total["count"] += row["count"]
        total["request_bytes"] += row["bytes_in"]
        total["response_bytes"] += row["bytes_out"]
        if row["operation"] == "GetObject" and row["range"]:
            range_gets[row["run"]] += row["count"]
        if row["operation"] == "GetObject" and row["object_class"] == "segment_data":
            segment_gets[row["run"]][row["phase"]] += row["count"]

    releases = sorted({values["release"] for values in inputs.values()})
    result = {"releases": {}}
    for release in releases:
        runs = [run for run, values in inputs.items() if values["release"] == release]

        def med(source, key):
            return statistics.median(source[run][key] for run in runs)

        result["releases"][release] = {
            "requests": {
                "count_median": med(request_totals, "count"),
                "request_bytes_median": med(request_totals, "request_bytes"),
                "response_bytes_median": med(request_totals, "response_bytes"),
                "range_get_count_median": statistics.median(
                    range_gets[run] for run in runs
                ),
                "cold_restore_segment_get_count_median": statistics.median(
                    segment_gets[run]["cold_restore"] for run in runs
                ),
                "incremental_catchup_segment_get_count_median": statistics.median(
                    segment_gets[run]["incremental_catchup"] for run in runs
                ),
            },
            "remote_objects": {
                f"{key}_median": med(object_totals, key)
                for key in (
                    "metadata_objects",
                    "metadata_bytes",
                    "data_objects",
                    "data_bytes",
                    "total_objects",
                    "total_bytes",
                )
            },
            "local_state_bytes_median": statistics.median(
                inputs[run]["replica_state_bytes"] for run in runs
            ),
            "latency": {
                key: value
                for key, value in statistics_by_metric.items()
                if key.startswith(f"{release}.")
            },
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--access-key", required=True)
    arguments = parser.parse_args()
    directory = arguments.directory
    samples, intervals = load_metrics(directory / "samples.tsv")
    statistics_by_metric = summarize(samples)
    requests, noop_data_puts = load_audit(
        directory / "audit.jsonl", intervals, arguments.access_key
    )
    inputs = load_inputs(directory / "inputs.tsv")
    object_totals = load_object_totals(directory, inputs)
    equal = logical_equality(directory, inputs)
    context = {key: value for key, value in read_tsv(directory / "context.tsv")}
    summary = comparison_summary(statistics_by_metric, requests, object_totals, inputs)
    expected_runs = set(inputs)
    expected_phases = {"cold_restore", "incremental_catchup", "publication", "noop"}
    observed_phases = {(sample["run"], sample["metric"]) for sample in samples}
    audit_runs = {row["run"] for row in requests if row["phase"] in expected_phases}
    checks = {
        "all_phases_observed": all(
            (run, phase) in observed_phases
            for run in expected_runs
            for phase in expected_phases
        ),
        "native_audit_observed": expected_runs <= audit_runs,
        "object_inventory_observed": all(
            object_totals.get(run, {}).get("total_objects", 0) > 0
            for run in expected_runs
        ),
        "noop_has_no_data_put": noop_data_puts == 0,
        "logical_trees_are_equal": equal,
    }
    verdict = "pass" if all(checks.values()) else "fail"
    report = {
        "format": "ofs-managed-sync-performance",
        "version": 1,
        "verdict": verdict,
        "context": context,
        "evidence": {
            "audit": "audit.jsonl",
            "commands": "commands.tsv",
            "inputs": "inputs.tsv",
            "runs": "runs/",
            "samples": "samples.tsv",
        },
        "audit_distribution": requests,
        "summary": summary,
        "checks": checks,
    }
    (directory / "results.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({"verdict": verdict, **summary, "checks": checks}, indent=2))
    raise SystemExit(verdict != "pass")


if __name__ == "__main__":
    main()
