"""Unit tests for the health endpoint."""

from __future__ import annotations

import threading
import urllib.request
from http.server import HTTPServer

import pytest

import app


@pytest.fixture
def health_server():
    server = HTTPServer(("127.0.0.1", 0), app.HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_healthz_returns_ok(health_server: int) -> None:
    with urllib.request.urlopen(f"http://127.0.0.1:{health_server}/healthz") as resp:
        assert resp.status == 200
        assert resp.read() == b"ok"


def test_unknown_path_returns_404(health_server: int) -> None:
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(f"http://127.0.0.1:{health_server}/nope")
    assert exc.value.code == 404
