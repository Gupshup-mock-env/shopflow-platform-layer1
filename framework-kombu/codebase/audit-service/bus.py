"""AMQP topology consumed by the audit service.

The exchange declaration mirrors the publisher's so that either side can be
deployed first. The queue is consumed by this service alone.
"""

from __future__ import annotations

from typing import Final

from kombu import Exchange, Queue

EVENT_EXCHANGE: Final[Exchange] = Exchange(
    "shopflow.events",
    type="topic",
    durable=True,
    auto_delete=False,
)

AUDIT_QUEUE: Final[Queue] = Queue(
    "audit-service.events",
    exchange=EVENT_EXCHANGE,
    routing_key="event.#",
    durable=True,
    auto_delete=False,
)
