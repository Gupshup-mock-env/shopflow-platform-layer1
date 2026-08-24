"""REST tests for catalog-service."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_healthz() -> None:
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_list_products_returns_the_seed_catalogue() -> None:
    resp = client.get("/products")
    assert resp.status_code == 200
    assert len(resp.json()) >= 3


def test_list_products_filters_by_category() -> None:
    resp = client.get("/products", params={"category": "kitchen"})
    ids = {p["product_id"] for p in resp.json()}
    assert "SKU-10431" in ids
    assert "SKU-20887" not in ids


def test_get_known_product() -> None:
    resp = client.get("/products/SKU-10431")
    assert resp.status_code == 200
    assert resp.json()["name"].startswith("Aeropress")


def test_get_unknown_product_is_404() -> None:
    assert client.get("/products/NOPE").status_code == 404


def test_upsert_product_creates_and_reads_back() -> None:
    body = {"name": "Pour-over Kettle", "price_cents": 6500, "category": "kitchen/coffee"}
    created = client.post("/products/SKU-99999", json=body)
    assert created.status_code == 201
    assert created.json()["product_id"] == "SKU-99999"
    assert client.get("/products/SKU-99999").json()["price_cents"] == 6500


def test_upsert_rejects_negative_price() -> None:
    resp = client.post(
        "/products/SKU-1", json={"name": "X", "price_cents": -1, "category": "misc"}
    )
    assert resp.status_code == 422
