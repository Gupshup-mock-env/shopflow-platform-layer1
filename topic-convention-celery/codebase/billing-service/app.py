"""ShopFlow billing service.

Enqueues invoice generation for orders that have settled, then stays resident
so the platform health probe keeps passing.
"""

from __future__ import annotations

import os
import signal
import threading
from typing import Final

from billing.broker import wait_for_broker
from billing.celery_app import destination_queue
from billing.tasks import generate_invoice
from billing.telemetry import log, start_health_server

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "billing-service")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

EVENT_COUNT: Final[int] = 5
EVENT_INTERVAL_SECONDS: Final[float] = 2.0

SETTLED_ORDERS: Final[tuple[tuple[str, int], ...]] = (
    ("ORD-001", 1999),
    ("ORD-002", 45050),
    ("ORD-003", 8725),
    ("ORD-004", 129900),
    ("ORD-005", 6499),
)

_shutdown = threading.Event()


def _handle_sigterm(signum: int, frame: object) -> None:
    _shutdown.set()


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)

    queue = destination_queue(generate_invoice.name)
    log(
        SERVICE_NAME,
        "started",
        topic=queue,
        task=generate_invoice.name,
        health_port=HEALTH_PORT,
    )

    wait_for_broker(SERVICE_NAME)

    published = 0
    for index, (order_id, amount_cents) in enumerate(SETTLED_ORDERS[:EVENT_COUNT]):
        if _shutdown.is_set():
            break
        result = generate_invoice.delay(order_id=order_id, amount_cents=amount_cents)
        published += 1
        log(
            SERVICE_NAME,
            "published",
            topic=queue,
            message_id=result.id,
            task=generate_invoice.name,
            order_id=order_id,
            amount_cents=amount_cents,
        )
        if index < EVENT_COUNT - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    log(SERVICE_NAME, "batch_complete", topic=queue, published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=queue, published=published)


if __name__ == "__main__":
    main()
