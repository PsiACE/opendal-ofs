#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

"""Small local implementation of the D1 query endpoint used by ofs."""

import argparse
import json
import re
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse


QUERY_PATH = re.compile(
    r"^(?:/client/v4)?/accounts/([^/]+)/d1/database/([^/]+)/query$"
)


def json_value(value):
    if isinstance(value, bytes):
        return list(value)
    return value


def sqlite_value(value):
    if isinstance(value, list) and all(
        isinstance(item, int) and 0 <= item <= 255 for item in value
    ):
        return bytes(value)
    if isinstance(value, (str, int, float, bytes)) or value is None:
        return value
    raise ValueError("unsupported SQL parameter")


class D1Handler(BaseHTTPRequestHandler):
    server_version = "ofs-local-d1/1"

    def do_GET(self):
        if urlparse(self.path).path == "/health":
            self.reply(200, {"ready": True})
        else:
            self.reply(404, {"success": False, "errors": [{"message": "not found"}]})

    def do_POST(self):
        match = QUERY_PATH.fullmatch(urlparse(self.path).path)
        if match is None:
            self.reply(404, {"success": False, "errors": [{"message": "not found"}]})
            return
        if self.headers.get("Authorization") != f"Bearer {self.server.token}":
            self.reply(401, {"success": False, "errors": [{"message": "unauthorized"}]})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length))
            statements = request.get("batch")
            if statements is None:
                statements = [{"sql": request["sql"], "params": request.get("params", [])}]
            result = self.execute(unquote(match.group(2)), statements)
        except (ValueError, KeyError, json.JSONDecodeError, sqlite3.Error) as error:
            self.reply(
                200,
                {"success": False, "result": [], "errors": [{"message": str(error)}]},
            )
            return
        self.reply(200, {"success": True, "result": result, "errors": []})

    def execute(self, database_id, statements):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", database_id):
            raise ValueError("invalid database id")
        if not isinstance(statements, list) or not statements:
            raise ValueError("query batch must not be empty")
        database = self.server.database_root / f"{database_id}.sqlite3"
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        results = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            for statement in statements:
                sql = statement["sql"]
                params = [sqlite_value(value) for value in statement.get("params", [])]
                cursor = connection.execute(sql, params)
                rows = [
                    {key: json_value(row[key]) for key in row.keys()}
                    for row in cursor.fetchall()
                ]
                results.append(
                    {
                        "success": True,
                        "results": rows,
                        "meta": {"served_by_primary": True, "changes": cursor.rowcount},
                    }
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return results

    def reply(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, message, *arguments):
        print(f"{self.address_string()} - {message % arguments}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--database-root", type=Path, required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()
    args.database_root.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((args.host, args.port), D1Handler)
    server.database_root = args.database_root
    server.token = args.token
    server.serve_forever()


if __name__ == "__main__":
    main()
