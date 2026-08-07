#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

"""Forward local S3 traffic and record what MinIO actually receives."""

import argparse
import http.client
import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class Server(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True


class RequestLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def append(self, record: dict[str, object]) -> None:
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        with self.lock, self.path.open("a", encoding="utf-8") as output:
            output.write(line + "\n")


def handler(upstream_host: str, upstream_port: int, request_log: RequestLog):
    class Proxy(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            super().setup()
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.upstream = http.client.HTTPConnection(
                upstream_host, upstream_port, timeout=30
            )
            self.upstream_requests = 0
            self.upstream_used_ns = time.monotonic_ns()

        def finish(self) -> None:
            self.upstream.close()
            super().finish()

        def read_body(self) -> bytes:
            if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
                length = int(self.headers.get("Content-Length", "0"))
                return self.rfile.read(length) if length else b""
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";", 1)[0], 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    return b"".join(chunks)
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)

        def forward(self) -> None:
            started_ns = time.time_ns()
            request_body = self.read_body()
            headers = {
                name: value
                for name, value in self.headers.items()
                if name.lower() not in HOP_HEADERS
            }
            headers["Content-Length"] = str(len(request_body))
            status = 502
            response_body = b""
            try:
                now = time.monotonic_ns()
                if (
                    self.upstream_requests >= 128
                    or now - self.upstream_used_ns >= 1_000_000_000
                ):
                    self.upstream.close()
                    self.upstream_requests = 0
                attempts = 2 if self.command in {"GET", "HEAD"} else 1
                for attempt in range(attempts):
                    try:
                        self.upstream.request(
                            self.command, self.path, request_body, headers
                        )
                        response = self.upstream.getresponse()
                        break
                    except (OSError, http.client.HTTPException):
                        self.upstream.close()
                        self.upstream_requests = 0
                        if attempt + 1 == attempts:
                            raise
                self.upstream_requests += 1
                self.upstream_used_ns = time.monotonic_ns()
                status = response.status
                response_length = response.getheader("Content-Length")
                response_body = response.read()
                self.send_response(status, response.reason)
                for name, value in response.getheaders():
                    if name.lower() not in HOP_HEADERS | {"content-length"}:
                        self.send_header(name, value)
                if self.command != "HEAD" or response_length is None:
                    response_length = str(len(response_body))
                self.send_header("Content-Length", response_length)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response_body)
                    self.wfile.flush()
            except BrokenPipeError:
                pass
            except (OSError, http.client.HTTPException) as error:
                self.upstream.close()
                response_body = str(error).encode()
                try:
                    self.send_error(502, explain=str(error))
                except BrokenPipeError:
                    pass
            finally:
                request_log.append(
                    {
                        "bytes_in": len(request_body),
                        "bytes_out": len(response_body),
                        "end_ns": time.time_ns(),
                        "method": self.command,
                        "path": self.path,
                        "start_ns": started_ns,
                        "status": status,
                    }
                )

        do_DELETE = forward
        do_GET = forward
        do_HEAD = forward
        do_POST = forward
        do_PUT = forward

        def log_message(self, _format: str, *_arguments: object) -> None:
            return

    return Proxy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True, help="HOST:PORT")
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--ready", required=True, type=Path)
    arguments = parser.parse_args()
    host, port = arguments.upstream.rsplit(":", 1)
    server = Server(
        ("127.0.0.1", 0),
        handler(host, int(port), RequestLog(arguments.log)),
    )
    arguments.ready.write_text(str(server.server_port), encoding="utf-8")
    server.serve_forever()


if __name__ == "__main__":
    main()
