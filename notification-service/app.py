"""ShopFlow notification service.

Turns paid orders into customer notifications and publishes them onto the
notification exchange. A short burst of sample confirmations is emitted on
startup, after which the process stays resident so the platform health probe
keeps passing.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final
from uuid import uuid4

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError, NackError, UnroutableError
from pika.exchange_type import ExchangeType

from models import OrderConfirmedNotification

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "notification-service")
RABBITMQ_HOST: Final[str] = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: Final[int] = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER: Final[str] = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD: Final[str] = os.environ.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST: Final[str] = os.environ.get("RABBITMQ_VHOST", "/")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

NOTIFICATION_COUNT: Final[int] = 5
NOTIFICATION_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
HEARTBEAT_SECONDS: Final[int] = 60
PUBLISH_ATTEMPTS: Final[int] = 6
PUBLISH_RETRY_DELAY_SECONDS: Final[float] = 5.0

CONFIRMATIONS: Final[tuple[tuple[str, str, str, str], ...]] = (
    (
        "ada.tolliver@example.com",
        "ORD-903150",
        "$75.00",
        "4x Espresso Cup, 4x Saucer",
    ),
    (
        "marcus.eze@example.com",
        "ORD-903151",
        "$89.90",
        "1x Pour Over Kettle",
    ),
    (
        "priya.raman@example.com",
        "ORD-903152",
        "$64.25",
        "2x Ethiopian Beans 1kg, 3x Paper Filters x100",
    ),
    (
        "jonas.hellstrom@example.com",
        "ORD-903153",
        "$117.49",
        "1x Hand Grinder, 1x Digital Scale",
    ),
    (
        "sofia.marchetti@example.com",
        "ORD-903154",
        "$70.39",
        "1x Cold Brew Jug, 2x Colombian Beans 500g, 1x Cleaning Tablets x24",
    ),
)

_shutdown = threading.Event()


def log(service: str, event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args: object) -> None:
        return


def start_health_server(port: int = 8080) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def _handle_sigterm(signum: int, frame: object) -> None:
    _shutdown.set()


def connection_parameters() -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
        heartbeat=HEARTBEAT_SECONDS,
        blocked_connection_timeout=30,
        connection_attempts=1,
        socket_timeout=5.0,
    )


def close_quietly(connection: pika.BlockingConnection | None) -> None:
    """Drop a connection that failed mid-setup without masking the cause."""
    if connection is None or connection.is_closed:
        return
    try:
        connection.close()
    except AMQPError:
        pass


def connect(
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> tuple[pika.BlockingConnection, BlockingChannel]:
    """Open a channel with the notification topology declared on it.

    The declarations are idempotent: re-running them against an existing
    exchange with matching arguments is a no-op on the broker.
    """
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while not _shutdown.is_set():
        attempt += 1
        connection: pika.BlockingConnection | None = None
        try:
            connection = pika.BlockingConnection(connection_parameters())
            channel = connection.channel()
            channel.exchange_declare(
                exchange="shopflow.notifications",
                exchange_type=ExchangeType.topic,
                durable=True,
            )
            channel.confirm_delivery()
        except AMQPError as exc:
            close_quietly(connection)
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"rabbitmq at {RABBITMQ_HOST}:{RABBITMQ_PORT} "
                    f"unreachable after {timeout:.0f}s"
                ) from exc
            log(
                SERVICE_NAME,
                "broker_unavailable",
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                attempt=attempt,
                retry_in_seconds=backoff,
                error=str(exc),
            )
            _shutdown.wait(backoff)
            backoff = min(backoff * 2, 5.0)
        else:
            log(
                SERVICE_NAME,
                "broker_ready",
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                attempts=attempt,
            )
            return connection, channel
    raise TimeoutError("shutdown requested before the broker became available")


def build_sample_notifications(
    count: int = NOTIFICATION_COUNT,
) -> list[OrderConfirmedNotification]:
    notifications: list[OrderConfirmedNotification] = []
    for index in range(count):
        recipient, order_id, order_total, items_summary = CONFIRMATIONS[
            index % len(CONFIRMATIONS)
        ]
        notifications.append(
            OrderConfirmedNotification(
                notification_id=str(uuid4()),
                recipient_email=recipient,
                order_id=order_id,
                order_total=order_total,
                items_summary=items_summary,
            )
        )
    return notifications


def publish_notification(
    channel: BlockingChannel,
    notification: OrderConfirmedNotification,
) -> bool:
    """Publish one confirmation, retrying while nothing is bound to route it.

    Publishes are mandatory and confirmed, so the broker returns the message
    instead of discarding it when no subscriber queue is bound yet. Each
    subscriber declares its own binding on startup, so a return early in the
    platform's lifecycle is transient and worth retrying.
    """
    properties = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=pika.DeliveryMode.Persistent,
        message_id=notification.notification_id,
        correlation_id=notification.order_id,
        app_id=SERVICE_NAME,
        type="order.confirmed",
        timestamp=int(time.time()),
    )
    for attempt in range(1, PUBLISH_ATTEMPTS + 1):
        try:
            channel.basic_publish(
                exchange="shopflow.notifications",
                routing_key="notification.email.order_confirmed",
                body=notification.to_json().encode("utf-8"),
                properties=properties,
                mandatory=True,
            )
        except UnroutableError:
            log(
                SERVICE_NAME,
                "unroutable",
                topic="shopflow.notifications",
                routing_key="notification.email.order_confirmed",
                message_id=notification.notification_id,
                order_id=notification.order_id,
                attempt=attempt,
            )
            if attempt == PUBLISH_ATTEMPTS or _shutdown.is_set():
                break
            _shutdown.wait(PUBLISH_RETRY_DELAY_SECONDS)
            continue
        except NackError as exc:
            log(
                SERVICE_NAME,
                "publish_failed",
                topic="shopflow.notifications",
                routing_key="notification.email.order_confirmed",
                message_id=notification.notification_id,
                error=str(exc),
            )
            return False
        log(
            SERVICE_NAME,
            "published",
            topic="shopflow.notifications",
            routing_key="notification.email.order_confirmed",
            message_id=notification.notification_id,
            order_id=notification.order_id,
            recipient_email=notification.recipient_email,
        )
        return True

    log(
        SERVICE_NAME,
        "publish_abandoned",
        topic="shopflow.notifications",
        routing_key="notification.email.order_confirmed",
        message_id=notification.notification_id,
        order_id=notification.order_id,
    )
    return False


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        health_port=HEALTH_PORT,
    )

    connection, channel = connect()

    published = 0
    notifications = build_sample_notifications()
    for index, notification in enumerate(notifications):
        if _shutdown.is_set():
            break
        if publish_notification(channel, notification):
            published += 1
        if index < len(notifications) - 1:
            _shutdown.wait(NOTIFICATION_INTERVAL_SECONDS)

    log(SERVICE_NAME, "batch_complete", published=published)

    while not _shutdown.is_set():
        try:
            connection.process_data_events(time_limit=1.0)
        except AMQPError as exc:
            log(SERVICE_NAME, "connection_lost", error=str(exc))
            if _shutdown.is_set():
                break
            connection, channel = connect()

    log(SERVICE_NAME, "stopping", published=published)
    close_quietly(connection)


if __name__ == "__main__":
    main()
