"""REST tests for the api-gateway.

The upstream forward is replaced with an async fake so routing is tested without
live upstream services.
"""

from __future__ import annotations

import httpx
from fastapi.testclient import TestClient

import app as gw_app

client = TestClient(gw_app.app)


def test_healthz() -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


def test_routes_lists_every_upstream() -> None:
    prefixes = {r["prefix"] for r in client.get("/routes").json()}
    assert {"/api/catalog", "/api/search", "/api/orders", "/api/inventory"} <= prefixes


def test_proxy_forwards_to_the_catalog_upstream(monkeypatch) -> None:
    captured: dict[str, str] = {}

    async def _fake_forward(method: str, url: str, body: bytes, params: bytes):
        captured["method"] = method
        captured["url"] = url
        return httpx.Response(
            200, content=b'{"ok":true}', headers={"content-type": "application/json"}
        )

    monkeypatch.setattr(gw_app, "forward", _fake_forward)

    resp = client.get("/api/catalog/products")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert captured["method"] == "GET"
    assert "catalog-service" in captured["url"]
    assert captured["url"].endswith("/products")


def test_proxy_unknown_service_is_404() -> None:
    assert client.get("/api/unknown/thing").status_code == 404
