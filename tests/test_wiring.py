"""Source-level wiring tests for the REST topology.

Assert that each caller references the upstreams it is supposed to reach, so the
service graph documented in the README matches the code.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _source(service: str) -> str:
    return (REPO_ROOT / service / "app.py").read_text(encoding="utf-8")


def test_order_service_calls_catalog_and_inventory() -> None:
    src = _source("order-service")
    assert "CATALOG_URL" in src
    assert "INVENTORY_URL" in src
    assert "/products/" in src
    assert "/reserve" in src


def test_search_service_calls_catalog() -> None:
    src = _source("search-service")
    assert "CATALOG_URL" in src
    assert "/products" in src


def test_gateway_fronts_every_service() -> None:
    src = _source("api-gateway")
    for upstream in ("CATALOG_URL", "SEARCH_URL", "ORDER_URL", "INVENTORY_URL"):
        assert upstream in src


@pytest.mark.parametrize(
    "service",
    ["api-gateway", "catalog-service", "inventory-service", "order-service", "search-service"],
)
def test_every_service_exposes_health(service: str) -> None:
    assert "/healthz" in _source(service)
