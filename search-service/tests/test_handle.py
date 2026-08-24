"""Unit tests for the search-service consume/index path."""

from __future__ import annotations

import pytest

import app


@pytest.fixture(autouse=True)
def _clear_index():
    app._index.clear()
    yield
    app._index.clear()


def _event_bytes(product_id: str = "SKU-1", price: int = 100) -> bytes:
    return (
        f'{{"product_id":"{product_id}","name":"Thing",'
        f'"price_cents":{price},"category":"misc"}}'
    ).encode("utf-8")


def test_handle_indexes_a_valid_event(make_message) -> None:
    msg = make_message(value=_event_bytes("SKU-42", 500), topic="shopflow.products.updated")
    app.handle(msg)
    assert app._index["SKU-42"]["price_cents"] == 500
    assert len(app._index) == 1


def test_handle_skips_an_invalid_payload(make_message) -> None:
    msg = make_message(value=b'{"product_id":"SKU-1"}', topic="shopflow.products.updated")
    app.handle(msg)
    assert app._index == {}


def test_handle_ignores_an_empty_message(make_message) -> None:
    msg = make_message(value=None, topic="shopflow.products.updated")
    app.handle(msg)
    assert app._index == {}


def test_handle_is_idempotent_on_product_id(make_message) -> None:
    app.handle(make_message(value=_event_bytes("SKU-7", 100)))
    app.handle(make_message(value=_event_bytes("SKU-7", 250)))
    assert len(app._index) == 1
    assert app._index["SKU-7"]["price_cents"] == 250


def test_header_value_reads_a_named_header(make_message) -> None:
    msg = make_message(
        value=_event_bytes(),
        headers=[("message-id", b"abc-123"), ("event-type", b"product.updated")],
    )
    assert app.header_value(msg, "message-id") == "abc-123"
    assert app.header_value(msg, "absent") is None
