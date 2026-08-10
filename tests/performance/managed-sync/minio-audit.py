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

"""Persist MinIO's native audit webhook events as JSON Lines."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class AuditLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def append(self, event: object) -> None:
        line = json.dumps(event, separators=(",", ":"), sort_keys=True)
        with self.lock, self.path.open("a", encoding="utf-8") as output:
            output.write(line + "\n")


def redact_credentials(event: dict[str, object]) -> dict[str, object]:
    headers = event.get("requestHeader")
    if isinstance(headers, dict):
        headers.pop("Authorization", None)
        headers.pop("X-Amz-Security-Token", None)
    event.pop("requestClaims", None)
    return event


def handler(audit_log: AuditLog):
    class AuditHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            try:
                length = int(self.headers["Content-Length"])
                payload = json.loads(self.rfile.read(length))
            except (KeyError, ValueError, json.JSONDecodeError):
                self.send_error(400, "expected one JSON audit event")
                return
            if not isinstance(payload, dict):
                self.send_error(400, "expected one JSON audit event")
                return
            audit_log.append(redact_credentials(payload))
            self.send_response(204)
            self.end_headers()

        def log_message(self, _format: str, *_arguments: object) -> None:
            return

    return AuditHandler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=0, type=int)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--ready", required=True, type=Path)
    arguments = parser.parse_args()

    arguments.log.touch()
    server = ThreadingHTTPServer(
        (arguments.host, arguments.port), handler(AuditLog(arguments.log))
    )
    server.daemon_threads = True
    arguments.ready.write_text(str(server.server_port), encoding="utf-8")
    server.serve_forever()


if __name__ == "__main__":
    main()
