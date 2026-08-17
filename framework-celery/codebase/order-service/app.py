"""ShopFlow order service.

Enqueues the payment capture task for a short burst of freshly placed orders
on startup and then stays resident so the platform health probe keeps passing.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from typing import Final

from kombu import Connection
from kombu.exceptions import OperationalError

from orders.celery_app import app, broker_url, resolve_queue
from orders.observability import log, start_health_server
from orders.tasks import process_payment

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "order-service")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

ENQUEUE_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

PLACED_ORDERS: Final[tuple[tuple[str, int], ...]] = (
    ("ORD-001", 4999),
    ("ORD-002", 12750),
    ("ORD-003", 899),
    ("ORD-004", 34500),
    ("ORD-005", 2199),
)

_shutdown = threading.Event()


def _handle_sigterm(signum: int, frame: object) -> None:
    _shutdown.set()


def wait_for_broker(timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS) -> None:
    """Block until the AMQP broker accepts a connection, or give up."""
    url = broker_url()
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while not _shutdown.is_set():
        attempt += 1
        try:
            connection = Connection(url, connect_timeout=5)
            try:
                connection.connect()
            finally:
                connection.release()
        except (OperationalError, OSError) as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"rabbitmq unreachable after {timeout:.0f}s"
                ) from exc
            log(
                SERVICE_NAME,
                "broker_unavailable",
                attempt=attempt,
                retry_in_seconds=backoff,
                error=str(exc),
            )
            _shutdown.wait(backoff)
            backoff = min(backoff * 2, 5.0)
        else:
            log(SERVICE_NAME, "broker_ready", attempts=attempt)
            return


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    queue = resolve_queue(process_payment.name)
    log(
        SERVICE_NAME,
        "started",
        topic=queue,
        task=process_payment.name,
        health_port=HEALTH_PORT,
    )

    wait_for_broker()

    enqueued = 0
    last = len(PLACED_ORDERS) - 1
    for index, (order_id, amount_cents) in enumerate(PLACED_ORDERS):
        if _shutdown.is_set():
            break
        result = process_payment.delay(order_id=order_id, amount_cents=amount_cents)
        enqueued += 1
        log(
            SERVICE_NAME,
            "published",
            topic=queue,
            message_id=result.id,
            task=process_payment.name,
            order_id=order_id,
            amount_cents=amount_cents,
        )
        if index < last:
            _shutdown.wait(ENQUEUE_INTERVAL_SECONDS)

    app.close()
    log(SERVICE_NAME, "batch_complete", topic=queue, published=enqueued)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=queue, published=enqueued)


if __name__ == "__main__":
    main()
