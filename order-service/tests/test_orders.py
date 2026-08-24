"""REST tests for order-service.

The service's downstream HTTP calls (catalog, inventory) are replaced with async
fakes so the orchestration logic is tested without a network.
"""

from __future__ import annotations

from fastapi import HTTPException
from fastapi.testclient import TestClient

import app as order_app

client = TestClient(order_app.app)


async def _fake_fetch_product(_client, product_id: str) -> dict:
    return {"product_id": product_id, "name": "X", "price_cents": 1000, "category": "misc"}


async def _fake_reserve(_client, product_id: str, quantity: int) -> dict:
    return {"product_id": product_id, "reserved": quantity, "on_hand": 100 - quantity}


def test_place_order_prices_from_catalog_and_reserves(monkeypatch) -> None:
    monkeypatch.setattr(order_app, "fetch_product", _fake_fetch_product)
    monkeypatch.setattr(order_app, "reserve_stock", _fake_reserve)

    resp = client.post(
        "/orders",
        json={
            "customer_id": "CUST-1",
            "lines": [
                {"product_id": "SKU-1", "quantity": 2},
                {"product_id": "SKU-2", "quantity": 1},
            ],
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["total_cents"] == 3000
    assert body["status"] == "placed"
    assert client.get(f"/orders/{body['order_id']}").status_code == 200


def test_place_order_unknown_product_is_400(monkeypatch) -> None:
    async def _missing(_client, product_id: str) -> dict:
        raise HTTPException(status_code=400, detail="unknown product")

    monkeypatch.setattr(order_app, "fetch_product", _missing)
    monkeypatch.setattr(order_app, "reserve_stock", _fake_reserve)

    resp = client.post(
        "/orders",
        json={"customer_id": "CUST-1", "lines": [{"product_id": "NOPE", "quantity": 1}]},
    )
    assert resp.status_code == 400


def test_place_order_insufficient_stock_is_409(monkeypatch) -> None:
    async def _deny(_client, product_id: str, quantity: int) -> dict:
        raise HTTPException(status_code=409, detail="insufficient stock")

    monkeypatch.setattr(order_app, "fetch_product", _fake_fetch_product)
    monkeypatch.setattr(order_app, "reserve_stock", _deny)

    resp = client.post(
        "/orders",
        json={"customer_id": "CUST-1", "lines": [{"product_id": "SKU-1", "quantity": 999}]},
    )
    assert resp.status_code == 409


def test_get_unknown_order_is_404() -> None:
    assert client.get("/orders/NOPE").status_code == 404


def test_place_order_requires_at_least_one_line() -> None:
    resp = client.post("/orders", json={"customer_id": "CUST-1", "lines": []})
    assert resp.status_code == 422
