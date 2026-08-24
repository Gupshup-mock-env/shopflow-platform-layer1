"""Unit tests for broker-connect retry behaviour."""

from __future__ import annotations

import pytest

import app


class ReadyProducer:
    def list_topics(self, timeout: float | None = None) -> object:
        return object()


class UnreachableProducer:
    def list_topics(self, timeout: float | None = None) -> object:
        raise app.KafkaException("no broker")


def test_wait_for_broker_returns_when_metadata_succeeds() -> None:
    assert app.wait_for_broker(ReadyProducer(), timeout=5.0) is None


def test_wait_for_broker_times_out_when_broker_never_answers() -> None:
    with pytest.raises(TimeoutError):
        app.wait_for_broker(UnreachableProducer(), timeout=0.0)
