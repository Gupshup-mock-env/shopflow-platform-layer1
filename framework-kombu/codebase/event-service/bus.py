"""AMQP topology for the ShopFlow domain event bus.

Every service that publishes onto the bus declares the same exchange, so the
declaration is idempotent regardless of which service starts first.
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

ORDER_CREATED_ROUTING_KEY: Final[str] = "event.order.created"

# Subscriber queues, declared on publish as well as by the service that owns
# them: a topic exchange drops anything published before a binding exists, and
# subscribers do not necessarily come up before the first transaction commits.
BOUND_QUEUES: Final[tuple[Queue, ...]] = (
    Queue(
        "audit-service.events",
        exchange=EVENT_EXCHANGE,
        routing_key="event.#",
        durable=True,
        auto_delete=False,
    ),
)
