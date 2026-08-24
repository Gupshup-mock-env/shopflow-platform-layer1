"""Unit tests for the consumed catalogue event model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from models import ProductUpdatedEvent


def test_validate_json_accepts_a_well_formed_event() -> None:
    raw = b'{"product_id":"SKU-1","name":"Thing","price_cents":100,"category":"misc"}'
    event = ProductUpdatedEvent.model_validate_json(raw)
    assert event.product_id == "SKU-1"
    assert event.price_cents == 100


def test_validate_json_rejects_a_negative_price() -> None:
    raw = b'{"product_id":"SKU-1","name":"Thing","price_cents":-5,"category":"misc"}'
    with pytest.raises(ValidationError):
        ProductUpdatedEvent.model_validate_json(raw)


def test_validate_json_rejects_malformed_json() -> None:
    with pytest.raises(ValidationError):
        ProductUpdatedEvent.model_validate_json(b"not json")
