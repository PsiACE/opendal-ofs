# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Aggregate MinIO audit events for Managed Sync evaluation."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


PRODUCT_ACCESS_KEY = "ofs-evaluation"


class AuditState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()
        self.markers: set[str] = set()
        self.volumes: dict[str, dict[str, object]] = {}

    def record(self, events: list[dict[str, object]]) -> None:
        with self.lock:
            for event in events:
                self._record(event)
            self._persist()

    def _record(self, event: dict[str, object]) -> None:
        if event.get("accessKey") != PRODUCT_ACCESS_KEY:
            return
        request_path = event.get("requestPath")
        if not isinstance(request_path, str):
            return
        parts = request_path.split("/")
        if len(parts) < 4 or parts[1] != "managed-sync":
            return
        if parts[2] == "audit-barrier":
            self.markers.add(parts[3])
            return
        if parts[2] not in {"calibration", "scale"}:
            return
        root = "/".join(parts[2:4])
        volume = self.volumes.setdefault(
            root,
            {"requests": 0, "request_bytes": 0, "response_bytes": 0, "groups": {}},
        )
        api = event.get("api")
        if not isinstance(api, dict):
            return
        operation = api.get("name", "unknown")
        status = api.get("statusCode", 0)
        received = api.get("rx", 0)
        sent = api.get("tx", 0)
        if not all(isinstance(value, int) for value in (status, received, sent)):
            return
        headers = event.get("requestHeader")
        ranged = isinstance(headers, dict) and "Range" in headers
        group_name = "|".join(
            (
                str(operation),
                str(status),
                object_class(request_path),
                "range" if ranged else "complete",
            )
        )
        groups = volume["groups"]
        assert isinstance(groups, dict)
        group = groups.setdefault(
            group_name,
            {"requests": 0, "request_bytes": 0, "response_bytes": 0},
        )
        for total in (volume, group):
            total["requests"] += 1
            total["request_bytes"] += received
            total["response_bytes"] += sent

    def _persist(self) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(
                {"markers": sorted(self.markers), "volumes": self.volumes},
                separators=(",", ":"),
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        temporary.replace(self.path)


def object_class(key: str) -> str:
    if "/objects/raw/" in key or ".ofs/managed/data/" in key:
        return "raw"
    if any(
        marker in key
        for marker in (
            "/objects/meta/",
            "/objects/commit/",
            ".ofs/managed/metadata/",
        )
    ):
        return "metadata"
    return "control"


def handler(audit_state: AuditState) -> type[BaseHTTPRequestHandler]:
    class AuditHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            try:
                length = int(self.headers["Content-Length"])
                payload = self.rfile.read(length)
                events = [json.loads(line) for line in payload.splitlines() if line]
            except (KeyError, ValueError, json.JSONDecodeError):
                self.send_error(400, "expected newline-delimited JSON audit events")
                return
            if not events or not all(isinstance(event, dict) for event in events):
                self.send_error(400, "expected JSON object audit events")
                return
            audit_state.record(events)
            self.send_response(204)
            self.end_headers()

        def log_message(self, _format: str, *_arguments: object) -> None:
            return

    return AuditHandler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--ready", required=True, type=Path)
    args = parser.parse_args()

    audit_state = AuditState(args.state)
    audit_state.record([])
    server = ThreadingHTTPServer(("0.0.0.0", 8080), handler(audit_state))
    server.daemon_threads = True
    args.ready.write_text("ready\n", encoding="utf-8")
    server.serve_forever()


if __name__ == "__main__":
    main()
