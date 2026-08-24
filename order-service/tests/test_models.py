"""Unit tests for the order event models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from models import OrderLine, OrderPlacedEvent


def _order(**overrides) -> OrderPlacedEvent:
    base = {
        "order_id": "ORD-1",
        "customer_id": "CUST-1",
        "lines": [
            {"product_id": "SKU-1", "quantity": 2, "unit_price_cents": 1000},
            {"product_id": "SKU-2", "quantity": 1, "unit_price_cents": 500},
        ],
    }
    base.update(overrides)
    return OrderPlacedEvent(**base)


def test_total_cents_sums_all_lines() -> None:
    assert _order().total_cents == 2 * 1000 + 1 * 500


def test_default_currency_is_usd() -> None:
    assert _order().currency == "USD"


def test_at_least_one_line_is_required() -> None:
    with pytest.raises(ValidationError):
        _order(lines=[])


def test_line_quantity_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        OrderLine(product_id="SKU-1", quantity=0, unit_price_cents=100)


def test_currency_must_be_three_chars() -> None:
    with pytest.raises(ValidationError):
        _order(currency="US")


def test_event_round_trips_through_json() -> None:
    event = _order()
    restored = OrderPlacedEvent.model_validate_json(event.model_dump_json())
    assert restored == event
