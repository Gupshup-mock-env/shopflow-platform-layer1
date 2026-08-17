"""Audit service.

Writes an immutable record of every ShopFlow domain event to the compliance
audit log. The queue binds the whole `event.#` space so new event types are
captured without a code change.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Final

from kombu import Connection, Consumer
from kombu.message import Message

from bus import AUDIT_QUEUE, EVENT_EXCHANGE

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "audit-service")
RABBITMQ_HOST: Final[str] = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: Final[int] = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER: Final[str] = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD: Final[str] = os.environ.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST: Final[str] = os.environ.get("RABBITMQ_VHOST", "/")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

DRAIN_TIMEOUT_SECONDS: Final[float] = 1.0
PREFETCH_COUNT: Final[int] = 10
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

_shutdown = threading.Event()
_consumed = 0


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


def build_connection() -> Connection:
    return Connection(
        hostname=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        userid=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD,
        virtual_host=RABBITMQ_VHOST,
        transport="amqp",
        heartbeat=30,
    )


def wait_for_broker(
    connection: Connection,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the AMQP handshake succeeds, or give up after `timeout`."""
    attempts = 0

    def on_error(exc: Exception, interval: float) -> None:
        nonlocal attempts
        attempts += 1
        log(
            SERVICE_NAME,
            "broker_unavailable",
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            attempt=attempts,
            retry_in_seconds=round(interval, 2),
            error=str(exc),
        )

    connection.ensure_connection(
        errback=on_error,
        interval_start=0.5,
        interval_step=0.5,
        interval_max=5.0,
        timeout=timeout,
        reraise_as_library_errors=False,
    )
    log(
        SERVICE_NAME,
        "broker_ready",
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        attempts=attempts,
    )


def record_audit_entry(body: dict[str, Any], message: Message) -> None:
    global _consumed

    delivery_info = message.delivery_info or {}
    log(
        SERVICE_NAME,
        "consumed",
        topic=EVENT_EXCHANGE.name,
        message_id=message.properties.get("message_id"),
        queue=AUDIT_QUEUE.name,
        routing_key=delivery_info.get("routing_key"),
        event_type=body.get("event_type"),
        order_id=body.get("order_id"),
        customer_id=body.get("customer_id"),
        total_cents=body.get("total_cents"),
        occurred_at=body.get("occurred_at"),
    )
    message.ack()
    _consumed += 1


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic=EVENT_EXCHANGE.name,
        queue=AUDIT_QUEUE.name,
        amqp_host=RABBITMQ_HOST,
        health_port=HEALTH_PORT,
    )

    with build_connection() as connection:
        wait_for_broker(connection)
        channel = connection.channel()
        consumer = Consumer(
            channel,
            queues=[AUDIT_QUEUE],
            callbacks=[record_audit_entry],
            accept=["json"],
            prefetch_count=PREFETCH_COUNT,
        )
        with consumer:
            log(
                SERVICE_NAME,
                "subscribed",
                topic=EVENT_EXCHANGE.name,
                queue=AUDIT_QUEUE.name,
                binding_pattern=AUDIT_QUEUE.routing_key,
            )
            try:
                while not _shutdown.is_set():
                    try:
                        connection.drain_events(timeout=DRAIN_TIMEOUT_SECONDS)
                    except socket.timeout:
                        continue
                    except OSError as exc:
                        log(SERVICE_NAME, "consume_error", queue=AUDIT_QUEUE.name, error=str(exc))
                        wait_for_broker(connection)
            finally:
                log(
                    SERVICE_NAME,
                    "stopping",
                    topic=EVENT_EXCHANGE.name,
                    queue=AUDIT_QUEUE.name,
                    consumed=_consumed,
                )


if __name__ == "__main__":
    main()
