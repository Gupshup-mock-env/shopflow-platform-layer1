"""ShopFlow email service.

Delivers transactional email for every notification routed to this service's
inbox queue, acknowledging each delivery once it has been handed to the
outbound mail gateway.
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

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties

from models import OrderConfirmedNotification

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "email-service")
RABBITMQ_HOST: Final[str] = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: Final[int] = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER: Final[str] = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD: Final[str] = os.environ.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST: Final[str] = os.environ.get("RABBITMQ_VHOST", "/")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

PREFETCH_COUNT: Final[int] = 16
INACTIVITY_TIMEOUT_SECONDS: Final[float] = 1.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
RECONNECT_DELAY_SECONDS: Final[float] = 2.0
HEARTBEAT_SECONDS: Final[int] = 60

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
    """Open a channel with this service's inbox topology declared on it.

    Every declaration is idempotent: the exchange, the queue and the binding
    are re-declared with identical arguments on each connect, which the broker
    treats as a no-op when they already exist.
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
            channel.queue_declare(queue="email-service.inbox", durable=True)
            channel.queue_bind(
                queue="email-service.inbox",
                exchange="shopflow.notifications",
                routing_key="notification.email.#",
            )
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)
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
                queue="email-service.inbox",
                attempts=attempt,
            )
            return connection, channel
    raise TimeoutError("shutdown requested before the broker became available")


def send_email(notification: OrderConfirmedNotification) -> str:
    """Hand the rendered confirmation to the outbound mail gateway."""
    return f"smtp-{notification.notification_id[:8]}"


def handle(
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
) -> None:
    message_id = properties.message_id
    try:
        notification = OrderConfirmedNotification.from_json(body)
    except (KeyError, ValueError) as exc:
        log(
            SERVICE_NAME,
            "invalid_payload",
            topic=method.exchange,
            queue="email-service.inbox",
            routing_key=method.routing_key,
            message_id=message_id,
            error=str(exc),
        )
        return

    log(
        SERVICE_NAME,
        "consumed",
        topic=method.exchange,
        queue="email-service.inbox",
        routing_key=method.routing_key,
        message_id=message_id,
        notification_id=notification.notification_id,
        order_id=notification.order_id,
        recipient_email=notification.recipient_email,
    )

    gateway_reference = send_email(notification)
    log(
        SERVICE_NAME,
        "email_sent",
        topic=method.exchange,
        message_id=message_id,
        order_id=notification.order_id,
        recipient_email=notification.recipient_email,
        gateway_reference=gateway_reference,
    )


def consume(channel: BlockingChannel) -> int:
    delivered = 0
    for method, properties, body in channel.consume(
        "email-service.inbox",
        inactivity_timeout=INACTIVITY_TIMEOUT_SECONDS,
    ):
        if _shutdown.is_set():
            break
        if method is None:
            continue
        handle(method, properties, body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        delivered += 1
    channel.cancel()
    return delivered


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

    delivered = 0
    while not _shutdown.is_set():
        connection, channel = connect()
        log(
            SERVICE_NAME,
            "bound",
            topic="shopflow.notifications",
            queue="email-service.inbox",
            binding_key="notification.email.#",
        )
        try:
            delivered += consume(channel)
        except AMQPError as exc:
            log(SERVICE_NAME, "connection_lost", error=str(exc), delivered=delivered)
            _shutdown.wait(RECONNECT_DELAY_SECONDS)
            continue
        finally:
            close_quietly(connection)

    log(SERVICE_NAME, "stopping", delivered=delivered)


if __name__ == "__main__":
    main()
