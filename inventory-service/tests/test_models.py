"""Unit tests for the inventory event models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from models import InventoryAdjustedEvent, OrderPlacedEvent


def test_order_placed_event_parses_from_json() -> None:
    raw = (
        b'{"order_id":"ORD-1","customer_id":"CUST-1","currency":"USD",'
        b'"lines":[{"product_id":"SKU-1","quantity":2,"unit_price_cents":100}]}'
    )
    event = OrderPlacedEvent.model_validate_json(raw)
    assert event.order_id == "ORD-1"
    assert event.lines[0].quantity == 2


def test_inventory_adjusted_event_round_trips() -> None:
    event = InventoryAdjustedEvent(
        product_id="SKU-1", order_id="ORD-1", delta=-2, on_hand=98
    )
    restored = InventoryAdjustedEvent.model_validate_json(event.model_dump_json())
    assert restored == event


def test_on_hand_cannot_be_negative() -> None:
    with pytest.raises(ValidationError):
        InventoryAdjustedEvent(product_id="SKU-1", order_id="ORD-1", delta=-2, on_hand=-1)
