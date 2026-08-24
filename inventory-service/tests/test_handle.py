"""Unit tests for the inventory consume -> republish path."""

from __future__ import annotations

import json

import pytest

import app


@pytest.fixture(autouse=True)
def _reset_stock():
    app._on_hand.clear()
    yield
    app._on_hand.clear()


def _order_bytes() -> bytes:
    return (
        b'{"order_id":"ORD-1","customer_id":"CUST-1","currency":"USD",'
        b'"lines":[{"product_id":"SKU-1","quantity":2,"unit_price_cents":100},'
        b'{"product_id":"SKU-2","quantity":1,"unit_price_cents":200}]}'
    )


def test_handle_publishes_one_adjustment_per_line(producer, make_message) -> None:
    app.handle(make_message(value=_order_bytes(), topic="shopflow.orders.placed"), producer)
    assert len(producer.produced) == 2
    assert {r["topic"] for r in producer.produced} == {"shopflow.inventory.adjusted"}


def test_handle_reports_new_on_hand_in_payload(producer, make_message) -> None:
    app.handle(make_message(value=_order_bytes()), producer)
    payload = json.loads(producer.produced[0]["value"].decode("utf-8"))
    assert payload["product_id"] == "SKU-1"
    assert payload["delta"] == -2
    assert payload["on_hand"] == app.INITIAL_ON_HAND - 2


def test_handle_skips_invalid_payload(producer, make_message) -> None:
    app.handle(make_message(value=b'{"order_id":"ORD-1"}'), producer)
    assert producer.produced == []


def test_handle_ignores_empty_message(producer, make_message) -> None:
    app.handle(make_message(value=None), producer)
    assert producer.produced == []
