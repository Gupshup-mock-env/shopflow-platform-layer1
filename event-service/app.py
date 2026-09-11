"""Event service.

Owns the ShopFlow domain event bus. Business transactions are turned into
immutable event envelopes and published onto the shared topic exchange, where
each downstream consumer binds its own queue with its own routing pattern.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Final

from kombu import Connection, Producer

from bus import BOUND_QUEUES, EVENT_EXCHANGE, ORDER_CREATED_ROUTING_KEY

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "event-service")
RABBITMQ_HOST: Final[str] = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: Final[int] = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER: Final[str] = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD: Final[str] = os.environ.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST: Final[str] = os.environ.get("RABBITMQ_VHOST", "/")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

PUBLISH_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

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


SAMPLE_ORDERS: Final[list[dict[str, Any]]] = [
    {"order_id": "ORD-77301", "customer_id": "CUST-4471", "total_cents": 12995},
    {"order_id": "ORD-77302", "customer_id": "CUST-8820", "total_cents": 4599},
    {"order_id": "ORD-77303", "customer_id": "CUST-1207", "total_cents": 28450},
    {"order_id": "ORD-77304", "customer_id": "CUST-6633", "total_cents": 7899},
    {"order_id": "ORD-77305", "customer_id": "CUST-3018", "total_cents": 15625},
]


def build_connection() -> Connection:
    return Connection(
        hostname=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        userid=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD,
        virtual_host=RABBITMQ_VHOST,
        transport="amqp",
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


def build_event(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": "order.created",
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "total_cents": order["total_cents"],
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic=EVENT_EXCHANGE.name,
        exchange_type=EVENT_EXCHANGE.type,
        amqp_host=RABBITMQ_HOST,
        health_port=HEALTH_PORT,
    )

    published = 0
    with build_connection() as connection:
        wait_for_broker(connection)
        channel = connection.channel()
        EVENT_EXCHANGE(channel).declare()
        for queue in BOUND_QUEUES:
            queue(channel).declare()
        producer = Producer(channel)

        for order in SAMPLE_ORDERS:
            if _shutdown.is_set():
                break
            body = build_event(order)
            message_id = f"msg-{uuid.uuid4().hex[:12]}"
            producer.publish(
                body,
                exchange=EVENT_EXCHANGE,
                routing_key=ORDER_CREATED_ROUTING_KEY,
                serializer="json",
                content_encoding="utf-8",
                delivery_mode=2,
                headers={"event_type": body["event_type"]},
                message_id=message_id,
                retry=True,
                retry_policy={
                    "interval_start": 0.5,
                    "interval_step": 0.5,
                    "interval_max": 5.0,
                    "max_retries": 5,
                },
            )
            published += 1
            log(
                SERVICE_NAME,
                "published",
                topic=EVENT_EXCHANGE.name,
                message_id=message_id,
                routing_key=ORDER_CREATED_ROUTING_KEY,
                event_type=body["event_type"],
                order_id=body["order_id"],
                customer_id=body["customer_id"],
                total_cents=body["total_cents"],
            )
            _shutdown.wait(PUBLISH_INTERVAL_SECONDS)

        channel.close()

    log(SERVICE_NAME, "idle", topic=EVENT_EXCHANGE.name, published=published)
    _shutdown.wait()
    log(SERVICE_NAME, "stopping", topic=EVENT_EXCHANGE.name, published=published)


if __name__ == "__main__":
    main()
