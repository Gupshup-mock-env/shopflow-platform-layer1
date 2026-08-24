"""Unit tests for the stock-adjustment logic."""

from __future__ import annotations

import pytest

import app
from models import OrderPlacedEvent


@pytest.fixture(autouse=True)
def _reset_stock():
    app._on_hand.clear()
    yield
    app._on_hand.clear()


def test_apply_adjustment_draws_down_from_initial() -> None:
    remaining = app.apply_adjustment("SKU-1", 2)
    assert remaining == app.INITIAL_ON_HAND - 2


def test_apply_adjustment_floors_at_zero() -> None:
    remaining = app.apply_adjustment("SKU-1", app.INITIAL_ON_HAND + 50)
    assert remaining == 0


def test_apply_adjustment_accumulates_across_calls() -> None:
    app.apply_adjustment("SKU-1", 10)
    remaining = app.apply_adjustment("SKU-1", 5)
    assert remaining == app.INITIAL_ON_HAND - 15


def test_build_adjustments_emits_one_per_line() -> None:
    event = OrderPlacedEvent(
        order_id="ORD-1",
        customer_id="CUST-1",
        lines=[
            {"product_id": "SKU-1", "quantity": 2, "unit_price_cents": 100},
            {"product_id": "SKU-2", "quantity": 1, "unit_price_cents": 200},
        ],
    )
    adjustments = app.build_adjustments(event)
    assert [a.product_id for a in adjustments] == ["SKU-1", "SKU-2"]
    assert [a.delta for a in adjustments] == [-2, -1]
    assert all(a.order_id == "ORD-1" for a in adjustments)
