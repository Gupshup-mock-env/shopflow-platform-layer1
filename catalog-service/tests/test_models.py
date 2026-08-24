"""Unit tests for the catalogue event model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from models import ProductUpdatedEvent


def test_valid_event_round_trips_through_json() -> None:
    event = ProductUpdatedEvent(
        product_id="SKU-10431",
        name="Aeropress Go Travel Press",
        price_cents=3999,
        category="kitchen/coffee",
    )
    restored = ProductUpdatedEvent.model_validate_json(event.model_dump_json())
    assert restored == event


def test_negative_price_is_rejected() -> None:
    with pytest.raises(ValidationError):
        ProductUpdatedEvent(
            product_id="SKU-1",
            name="Bad",
            price_cents=-1,
            category="misc",
        )


def test_missing_field_is_rejected() -> None:
    with pytest.raises(ValidationError):
        ProductUpdatedEvent(product_id="SKU-1", name="No price", category="misc")


@pytest.mark.parametrize("price", [0, 1, 999, 100000])
def test_non_negative_prices_are_accepted(price: int) -> None:
    event = ProductUpdatedEvent(
        product_id="SKU-9",
        name="Edge",
        price_cents=price,
        category="misc",
    )
    assert event.price_cents == price
