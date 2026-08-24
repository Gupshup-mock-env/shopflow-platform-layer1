"""Unit tests for the order publish path."""

from __future__ import annotations

import json

import app
from models import OrderPlacedEvent


def _order() -> OrderPlacedEvent:
    return OrderPlacedEvent(
        order_id="ORD-88213",
        customer_id="CUST-2001",
        lines=[
            {"product_id": "SKU-10431", "quantity": 1, "unit_price_cents": 3999},
            {"product_id": "SKU-52260", "quantity": 2, "unit_price_cents": 1699},
        ],
    )


def test_publish_targets_the_orders_topic(producer) -> None:
    app.publish(producer, _order())
    record = producer.produced[0]
    assert record["topic"] == "shopflow.orders.placed"
    assert record["key"] == b"ORD-88213"


def test_publish_serialises_lines(producer) -> None:
    app.publish(producer, _order())
    payload = json.loads(producer.produced[0]["value"].decode("utf-8"))
    assert payload["order_id"] == "ORD-88213"
    assert len(payload["lines"]) == 2


def test_publish_sets_order_event_type(producer) -> None:
    app.publish(producer, _order())
    headers = dict(producer.produced[0]["headers"])
    assert headers["event-type"] == b"order.placed"
