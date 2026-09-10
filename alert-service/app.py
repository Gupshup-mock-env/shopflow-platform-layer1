"""ShopFlow alert service.

Raises the merchandising alerts detected by the last inventory sweep on
startup and then stays resident so the platform health probe keeps passing.
"""

from __future__ import annotations

import signal
import threading
from typing import Final

from alerting.actors import send_alert
from alerting.config import health_port, service_name
from alerting.observability import log, start_health_server, wait_for_broker

SERVICE_NAME: Final[str] = service_name("alert-service")
HEALTH_PORT: Final[int] = health_port()

SEND_INTERVAL_SECONDS: Final[float] = 2.0

DETECTED_ALERTS: Final[tuple[tuple[str, str], ...]] = (
    ("stockout", "SKU-789"),
    ("low_stock", "SKU-142"),
    ("price_drop", "SKU-908"),
    ("stockout", "SKU-455"),
    ("backorder", "SKU-663"),
)

_shutdown = threading.Event()


def _handle_sigterm(signum: int, frame: object) -> None:
    _shutdown.set()


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic=send_alert.queue_name,
        actor=send_alert.actor_name,
        health_port=HEALTH_PORT,
    )

    wait_for_broker(SERVICE_NAME, stop=_shutdown)

    sent = 0
    last = len(DETECTED_ALERTS) - 1
    for index, (alert_type, sku) in enumerate(DETECTED_ALERTS):
        if _shutdown.is_set():
            break
        message = send_alert.send(alert_type=alert_type, sku=sku)
        sent += 1
        log(
            SERVICE_NAME,
            "published",
            topic=send_alert.queue_name,
            message_id=message.message_id,
            actor=send_alert.actor_name,
            alert_type=alert_type,
            sku=sku,
        )
        if index < last:
            _shutdown.wait(SEND_INTERVAL_SECONDS)

    log(SERVICE_NAME, "batch_complete", topic=send_alert.queue_name, published=sent)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=send_alert.queue_name, published=sent)
    send_alert.broker.close()


if __name__ == "__main__":
    main()
