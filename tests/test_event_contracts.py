"""Cross-service schema-contract tests.

A producer and its consumer keep separate copies of the shared event model. These
tests assert the copies stay compatible: a payload emitted by the producer must
validate cleanly against the consumer's model.
"""

from __future__ import annotations

import pytest


def test_order_payload_from_producer_validates_against_consumer(load_models) -> None:
    order_models = load_models("order-service")
    inventory_models = load_models("inventory-service")

    produced = order_models.OrderPlacedEvent(
        order_id="ORD-1",
        customer_id="CUST-1",
        lines=[{"product_id": "SKU-1", "quantity": 2, "unit_price_cents": 100}],
    )

    # The consumer must accept exactly what the producer emitted.
    consumed = inventory_models.OrderPlacedEvent.model_validate_json(
        produced.model_dump_json()
    )
    assert consumed.order_id == "ORD-1"
    assert consumed.lines[0].product_id == "SKU-1"


def test_product_payload_shared_between_catalog_and_search(load_models) -> None:
    catalog_models = load_models("catalog-service")
    search_models = load_models("search-service")

    produced = catalog_models.ProductUpdatedEvent(
        product_id="SKU-1", name="Thing", price_cents=100, category="misc"
    )
    consumed = search_models.ProductUpdatedEvent.model_validate_json(
        produced.model_dump_json()
    )
    assert consumed == search_models.ProductUpdatedEvent(
        product_id="SKU-1", name="Thing", price_cents=100, category="misc"
    )


def test_order_and_inventory_agree_on_line_fields(load_models) -> None:
    order_models = load_models("order-service")
    inventory_models = load_models("inventory-service")

    order_fields = set(order_models.OrderLine.model_fields)
    inventory_fields = set(inventory_models.OrderLine.model_fields)
    assert order_fields == inventory_fields
