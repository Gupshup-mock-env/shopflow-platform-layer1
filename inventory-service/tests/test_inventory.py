"""REST tests for inventory-service."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_healthz() -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


def test_get_stock_defaults_to_initial_level() -> None:
    resp = client.get("/stock/SKU-A")
    assert resp.status_code == 200
    assert resp.json()["on_hand"] == 100


def test_reserve_reduces_on_hand() -> None:
    resp = client.post("/stock/SKU-B/reserve", json={"quantity": 10})
    assert resp.status_code == 200
    assert resp.json()["reserved"] == 10
    assert resp.json()["on_hand"] == 90
    assert client.get("/stock/SKU-B").json()["on_hand"] == 90


def test_reserve_more_than_available_is_409() -> None:
    client.post("/stock/SKU-C/reserve", json={"quantity": 100})
    resp = client.post("/stock/SKU-C/reserve", json={"quantity": 1})
    assert resp.status_code == 409


def test_reserve_rejects_non_positive_quantity() -> None:
    assert client.post("/stock/SKU-D/reserve", json={"quantity": 0}).status_code == 422
