#!/usr/bin/env python3
"""Fail when ZLS reports a diagnostic for a project Zig source file."""

from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
TIMEOUT_SECONDS = 30
QUIET_SECONDS = 2


def source_files() -> list[Path]:
    files = [ROOT / "build.zig"]
    files.extend((ROOT / "src").rglob("*.zig"))
    files.extend((ROOT / "examples").rglob("*.zig"))
    return sorted(path for path in files if path.is_file())


def write_message(process: subprocess.Popen[bytes], message: dict[str, Any]) -> None:
    payload = json.dumps(message, separators=(",", ":")).encode()
    assert process.stdin is not None
    process.stdin.write(f"Content-Length: {len(payload)}\r\n\r\n".encode())
    process.stdin.write(payload)
    process.stdin.flush()


def read_message(stream: Any) -> dict[str, Any] | None:
    content_length = None
    while True:
        line = stream.readline()
        if not line:
            return None
        if line == b"\r\n":
            break
        name, _, value = line.decode().partition(":")
        if name.lower() == "content-length":
            content_length = int(value.strip())

    if content_length is None:
        raise RuntimeError("ZLS response did not include Content-Length")
    return json.loads(stream.read(content_length))


def collect_messages(
    process: subprocess.Popen[bytes],
    messages: queue.Queue[dict[str, Any] | BaseException | None],
) -> None:
    assert process.stdout is not None
    try:
        while True:
            message = read_message(process.stdout)
            messages.put(message)
            if message is None:
                return
    except BaseException as error:
        messages.put(error)


def receive(
    messages: queue.Queue[dict[str, Any] | BaseException | None],
    deadline: float,
) -> dict[str, Any]:
    timeout = deadline - time.monotonic()
    if timeout <= 0:
        raise TimeoutError("timed out waiting for ZLS diagnostics")
    item = messages.get(timeout=timeout)
    if item is None:
        raise RuntimeError("ZLS exited before linting completed")
    if isinstance(item, BaseException):
        raise item
    return item


def main() -> int:
    try:
        process = subprocess.Popen(
            ["zls", "--config-path", str(ROOT / "zls.json")],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        print("error: zls was not found in PATH", file=sys.stderr)
        return 2

    messages: queue.Queue[dict[str, Any] | BaseException | None] = queue.Queue()
    threading.Thread(
        target=collect_messages,
        args=(process, messages),
        daemon=True,
    ).start()

    root_uri = ROOT.as_uri()
    write_message(
        process,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "processId": os.getpid(),
                "rootUri": root_uri,
                "capabilities": {
                    "textDocument": {
                        "publishDiagnostics": {
                            "relatedInformation": True,
                            "versionSupport": True,
                        }
                    },
                },
                "initializationOptions": {
                    "enable_build_on_save": False,
                    "warn_style": True,
                },
            },
        },
    )

    deadline = time.monotonic() + TIMEOUT_SECONDS
    while receive(messages, deadline).get("id") != 1:
        pass

    write_message(process, {"jsonrpc": "2.0", "method": "initialized", "params": {}})
    write_message(
        process,
        {
            "jsonrpc": "2.0",
            "method": "workspace/didChangeConfiguration",
            "params": {
                "settings": {
                    "enable_build_on_save": False,
                    "warn_style": True,
                }
            },
        },
    )

    files_by_uri: dict[str, Path] = {}
    for path in source_files():
        uri = path.as_uri()
        files_by_uri[uri] = path
        write_message(
            process,
            {
                "jsonrpc": "2.0",
                "method": "textDocument/didOpen",
                "params": {
                    "textDocument": {
                        "uri": uri,
                        "languageId": "zig",
                        "version": 1,
                        "text": path.read_text(),
                    }
                },
            },
        )
        write_message(
            process,
            {
                "jsonrpc": "2.0",
                "method": "textDocument/didSave",
                "params": {"textDocument": {"uri": uri}},
            },
        )

    diagnostics: list[tuple[Path, dict[str, Any]]] = []
    analysis_deadline = time.monotonic() + TIMEOUT_SECONDS
    quiet_deadline = time.monotonic() + QUIET_SECONDS
    while time.monotonic() < analysis_deadline:
        timeout = min(analysis_deadline, quiet_deadline) - time.monotonic()
        if timeout <= 0:
            break
        try:
            item = messages.get(timeout=timeout)
        except queue.Empty:
            break
        if item is None:
            raise RuntimeError("ZLS exited before linting completed")
        if isinstance(item, BaseException):
            raise item
        message = item
        if message.get("method") != "textDocument/publishDiagnostics":
            continue
        quiet_deadline = time.monotonic() + QUIET_SECONDS
        params = message["params"]
        uri = params["uri"]
        path = files_by_uri.get(uri)
        if path is None:
            continue
        for diagnostic in params.get("diagnostics", []):
            diagnostics.append((path, diagnostic))

    write_message(
        process,
        {"jsonrpc": "2.0", "id": 2, "method": "shutdown", "params": None},
    )
    while receive(messages, deadline).get("id") != 2:
        pass
    write_message(process, {"jsonrpc": "2.0", "method": "exit", "params": None})
    process.wait(timeout=5)

    severity_names = {1: "error", 2: "warning", 3: "information", 4: "hint"}
    for path, diagnostic in diagnostics:
        start = diagnostic["range"]["start"]
        severity = severity_names.get(diagnostic.get("severity"), "diagnostic")
        code = diagnostic.get("code")
        suffix = f" [{code}]" if code else ""
        relative = path.relative_to(ROOT)
        print(
            f"{relative}:{start['line'] + 1}:{start['character'] + 1}: "
            f"{severity}: {diagnostic['message']}{suffix}"
        )

    if diagnostics:
        print(f"ZLS lint failed with {len(diagnostics)} diagnostic(s).")
        return 1

    print(f"ZLS lint passed for {len(source_files())} file(s).")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, TimeoutError, queue.Empty) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2)
