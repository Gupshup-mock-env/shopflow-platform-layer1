"""Topic-wiring tests.

Assert that producers and consumers reference the same topic strings, so the
event flow documented in the README is actually the flow the code implements.
"""

from __future__ import annotations

from pathlib import Path

import pytest

PRODUCTS_TOPIC = "shopflow.products.updated"
ORDERS_TOPIC = "shopflow.orders.placed"
INVENTORY_TOPIC = "shopflow.inventory.adjusted"


def _source(repo_root: Path, service: str) -> str:
    return (repo_root / service / "app.py").read_text(encoding="utf-8")


def test_products_topic_connects_catalog_to_search(repo_root: Path) -> None:
    assert PRODUCTS_TOPIC in _source(repo_root, "catalog-service")
    assert PRODUCTS_TOPIC in _source(repo_root, "search-service")


def test_orders_topic_connects_order_to_inventory(repo_root: Path) -> None:
    assert ORDERS_TOPIC in _source(repo_root, "order-service")
    assert ORDERS_TOPIC in _source(repo_root, "inventory-service")


def test_inventory_topic_is_produced_by_inventory_service(repo_root: Path) -> None:
    assert INVENTORY_TOPIC in _source(repo_root, "inventory-service")


@pytest.mark.parametrize(
    "service",
    ["catalog-service", "search-service", "order-service", "inventory-service"],
)
def test_every_service_exposes_a_health_endpoint(repo_root: Path, service: str) -> None:
    assert "/healthz" in _source(repo_root, service)
