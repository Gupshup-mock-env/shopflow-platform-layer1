"""REST tests for search-service.

The catalogue fetch is replaced with an async fake so filtering is tested
without a network round-trip to catalog-service.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

import app as search_app

client = TestClient(search_app.app)

_CATALOG = [
    {"product_id": "SKU-1", "name": "Coffee Press", "category": "kitchen/coffee", "price_cents": 3999},
    {"product_id": "SKU-2", "name": "Wool Socks", "category": "apparel/socks", "price_cents": 2450},
]


async def _fake_fetch_catalog(_client) -> list[dict]:
    return _CATALOG


def test_search_matches_on_name(monkeypatch) -> None:
    monkeypatch.setattr(search_app, "fetch_catalog", _fake_fetch_catalog)
    resp = client.get("/search", params={"q": "coffee"})
    assert resp.status_code == 200
    hits = resp.json()["hits"]
    assert [h["product_id"] for h in hits] == ["SKU-1"]


def test_search_matches_on_category(monkeypatch) -> None:
    monkeypatch.setattr(search_app, "fetch_catalog", _fake_fetch_catalog)
    hits = client.get("/search", params={"q": "apparel"}).json()["hits"]
    assert [h["product_id"] for h in hits] == ["SKU-2"]


def test_search_with_no_match_returns_empty(monkeypatch) -> None:
    monkeypatch.setattr(search_app, "fetch_catalog", _fake_fetch_catalog)
    assert client.get("/search", params={"q": "zzz"}).json()["hits"] == []


def test_search_requires_a_query() -> None:
    assert client.get("/search").status_code == 422
