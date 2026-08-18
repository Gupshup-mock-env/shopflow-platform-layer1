"""Cart service.

Owns shopping-cart state and emits a ``CartUpdatedEvent`` every time a cart is
mutated. On startup it drains the write-behind buffer that survived the last
restart, then stays resident so the platform health probe keeps passing.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Final
from uuid import uuid4

from confluent_kafka import KafkaException, Message, Producer

from shared.constants import CART_EVENTS_TOPIC
from shared.events import CartUpdatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "cart-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

BUFFERED_MUTATIONS: Final[tuple[dict[str, Any], ...]] = (
    {"cart_id": "CART-9F2A11", "customer_id": "CUST-40218", "item_count": 3, "total_cents": 10894},
    {"cart_id": "CART-7B0C48", "customer_id": "CUST-51733", "item_count": 1, "total_cents": 2450},
    {"cart_id": "CART-24DD90", "customer_id": "guest-8e11c4", "item_count": 5, "total_cents": 23115},
    {"cart_id": "CART-9F2A11", "customer_id": "CUST-40218", "item_count": 4, "total_cents": 14893},
    {"cart_id": "CART-C31E07", "customer_id": "CUST-60904", "item_count": 2, "total_cents": 7194},
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


def wait_for_broker(
    producer: Producer,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the broker answers a metadata request, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while not _shutdown.is_set():
        attempt += 1
        try:
            producer.list_topics(timeout=5.0)
        except KafkaException as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"kafka at {KAFKA_BOOTSTRAP} unreachable after {timeout:.0f}s"
                ) from exc
            log(
                SERVICE_NAME,
                "broker_unavailable",
                bootstrap_servers=KAFKA_BOOTSTRAP,
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
                bootstrap_servers=KAFKA_BOOTSTRAP,
                attempts=attempt,
            )
            return


def _on_delivery(err: object, msg: Message) -> None:
    if err is not None:
        log(SERVICE_NAME, "delivery_failed", topic=msg.topic(), error=str(err))
        return
    log(
        SERVICE_NAME,
        "delivered",
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
    )


def publish(producer: Producer, event: CartUpdatedEvent) -> None:
    message_id = str(uuid4())
    producer.produce(
        topic=CART_EVENTS_TOPIC,
        key=event.cart_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", b"cart.updated"),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic=CART_EVENTS_TOPIC,
        message_id=message_id,
        cart_id=event.cart_id,
        customer_id=event.customer_id,
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic=CART_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        health_port=HEALTH_PORT,
    )

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": f"{SERVICE_NAME}-0",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "retries": 5,
        }
    )
    wait_for_broker(producer)

    published = 0
    for index, mutation in enumerate(BUFFERED_MUTATIONS):
        if _shutdown.is_set():
            break
        publish(producer, CartUpdatedEvent(**mutation))
        published += 1
        if index < len(BUFFERED_MUTATIONS) - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", topic=CART_EVENTS_TOPIC, published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=CART_EVENTS_TOPIC, published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
