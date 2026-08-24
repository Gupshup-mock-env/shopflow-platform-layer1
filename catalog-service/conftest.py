"""Shared test fixtures for this service.

The service depends on ``confluent_kafka``, a C-extension we do not want to
require (nor a live broker) just to unit-test the application logic. This
conftest installs a lightweight in-memory fake into ``sys.modules`` *before* the
app module imports it, so tests exercise the real produce/consume/handle code
paths against an inspectable double.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Callable

import pytest


class FakeMessage:
    def __init__(
        self,
        value: bytes | None = None,
        *,
        topic: str = "t",
        partition: int = 0,
        offset: int = 0,
        headers: list[tuple[str, bytes]] | None = None,
        error: object | None = None,
    ) -> None:
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._headers = headers
        self._error = error

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def error(self) -> object | None:
        return self._error


class FakeProducer:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self.produced: list[dict[str, Any]] = []
        self.flush_calls = 0

    def list_topics(self, timeout: float | None = None) -> object:
        return types.SimpleNamespace(topics={})

    def produce(
        self,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        on_delivery: Callable[[object, FakeMessage], None] | None = None,
    ) -> None:
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )
        if on_delivery is not None:
            on_delivery(None, FakeMessage(value=value, topic=topic))

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flush_calls += 1
        return 0


class FakeConsumer:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self.subscribed: list[str] = []
        self.committed: list[object] = []
        self.closed = False
        self._queue: list[FakeMessage] = []

    def list_topics(self, timeout: float | None = None) -> object:
        return types.SimpleNamespace(topics={})

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def feed(self, msg: FakeMessage) -> None:
        self._queue.append(msg)

    def poll(self, timeout: float | None = None) -> FakeMessage | None:
        return self._queue.pop(0) if self._queue else None

    def commit(self, message: object = None, asynchronous: bool = True) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


class FakeKafkaError(Exception):
    _PARTITION_EOF = -191

    def __init__(self, code: int = 0) -> None:
        super().__init__(f"kafka error {code}")
        self._code = code

    def code(self) -> int:
        return self._code


class FakeKafkaException(Exception):
    pass


def _install_fake_confluent() -> types.ModuleType:
    module = types.ModuleType("confluent_kafka")
    module.Producer = FakeProducer
    module.Consumer = FakeConsumer
    module.Message = FakeMessage
    module.KafkaError = FakeKafkaError
    module.KafkaException = FakeKafkaException
    sys.modules["confluent_kafka"] = module
    return module


_install_fake_confluent()


@pytest.fixture
def producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def consumer() -> FakeConsumer:
    return FakeConsumer()


@pytest.fixture
def make_message() -> Callable[..., FakeMessage]:
    def _make(**kwargs: Any) -> FakeMessage:
        return FakeMessage(**kwargs)

    return _make
