"""Unit tests for the catalogue publish path."""

from __future__ import annotations

import json

import app
from models import ProductUpdatedEvent


def test_publish_targets_the_products_topic(producer) -> None:
    event = ProductUpdatedEvent(
        product_id="SKU-10431",
        name="Aeropress Go Travel Press",
        price_cents=3999,
        category="kitchen/coffee",
    )

    app.publish(producer, event)

    assert len(producer.produced) == 1
    record = producer.produced[0]
    assert record["topic"] == "shopflow.products.updated"
    assert record["key"] == b"SKU-10431"


def test_publish_serialises_the_event_as_json(producer) -> None:
    event = ProductUpdatedEvent(
        product_id="SKU-52260",
        name="USB-C 100W Braided Cable 2m",
        price_cents=1699,
        category="electronics/cables",
    )

    app.publish(producer, event)

    payload = json.loads(producer.produced[0]["value"].decode("utf-8"))
    assert payload["product_id"] == "SKU-52260"
    assert payload["price_cents"] == 1699


def test_publish_sets_event_type_header(producer) -> None:
    event = ProductUpdatedEvent(
        product_id="SKU-1",
        name="Thing",
        price_cents=100,
        category="misc",
    )

    app.publish(producer, event)

    headers = dict(producer.produced[0]["headers"])
    assert headers["event-type"] == b"product.updated"
    assert headers["content-type"] == b"application/json"
    assert "message-id" in headers
