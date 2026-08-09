#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

"""Build a deterministic, bounded fixture with the measured agent-home shape."""

import hashlib
import json
import pathlib
import sys


SHAPE = {
    ".agents": {"directories": 11, "sizes": ((7, 512), (27, 4 * 1024))},
    ".bub": {
        "directories": 591,
        "sizes": (
            (2401, 512),
            (2858, 4 * 1024),
            (578, 64 * 1024),
            (12, 256 * 1024),
        ),
    },
    ".codex": {
        "directories": 3230,
        "sizes": (
            (1579, 512),
            (4241, 4 * 1024),
            (644, 64 * 1024),
            (133, 256 * 1024),
        ),
    },
}


def deterministic_bytes(seed: str, size: int) -> bytes:
    return hashlib.shake_256(seed.encode()).digest(size)


def build(root: pathlib.Path) -> None:
    for domain, shape in SHAPE.items():
        domain_root = root / domain
        domain_root.mkdir(parents=True)
        directories = [
            domain_root / f"d{index:04d}"
            for index in range(shape["directories"] - 1)
        ]
        for directory in directories:
            directory.mkdir(parents=True)
        file_index = 0
        for count, size in shape["sizes"]:
            for _ in range(count):
                path = directories[file_index % len(directories)] / f"f{file_index:05d}.bin"
                path.write_bytes(deterministic_bytes(f"{domain}-{file_index}", size))
                file_index += 1


def describe() -> dict[str, object]:
    domains = {}
    for domain, shape in SHAPE.items():
        domains[domain] = {
            "directories": shape["directories"],
            "files": sum(count for count, _ in shape["sizes"]),
            "bytes": sum(count * size for count, size in shape["sizes"]),
            "size_buckets": [
                {"files": count, "representative_bytes": size}
                for count, size in shape["sizes"]
            ],
        }
    return {"source": "measured local agent-home distribution", "domains": domains}


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in {"build", "describe"}:
        raise SystemExit(f"usage: {sys.argv[0]} build DIRECTORY | describe OUTPUT")
    target = pathlib.Path(sys.argv[2])
    if sys.argv[1] == "build":
        build(target)
    else:
        target.write_text(json.dumps(describe(), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
