"""Catalogue service.

Publishes a ``ProductUpdatedEvent`` whenever a merchandiser edits a product.
On startup the service replays the pending edit queue so that downstream
indexes converge after a deployment, then stays resident so the platform
health probe keeps passing.
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

from models import ProductUpdatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "catalog-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

PENDING_EDITS: Final[tuple[dict[str, Any], ...]] = (
    {
        "product_id": "SKU-10431",
        "name": "Aeropress Go Travel Press",
        "price_cents": 3999,
        "category": "kitchen/coffee",
    },
    {
        "product_id": "SKU-20887",
        "name": "Merino Wool Crew Socks, 3 pack",
        "price_cents": 2450,
        "category": "apparel/socks",
    },
    {
        "product_id": "SKU-33125",
        "name": "Cast Iron Skillet 12 inch",
        "price_cents": 5495,
        "category": "kitchen/cookware",
    },
    {
        "product_id": "SKU-41902",
        "name": "Trailhead 28L Daypack",
        "price_cents": 8900,
        "category": "outdoor/packs",
    },
    {
        "product_id": "SKU-52260",
        "name": "USB-C 100W Braided Cable 2m",
        "price_cents": 1699,
        "category": "electronics/cables",
    },
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


def publish(producer: Producer, event: ProductUpdatedEvent) -> None:
    message_id = str(uuid4())
    producer.produce(
        topic="shopflow.products.updated",
        key=event.product_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", b"product.updated"),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic="shopflow.products.updated",
        message_id=message_id,
        product_id=event.product_id,
        category=event.category,
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic="shopflow.products.updated",
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
    for index, edit in enumerate(PENDING_EDITS):
        if _shutdown.is_set():
            break
        publish(producer, ProductUpdatedEvent(**edit))
        published += 1
        if index < len(PENDING_EDITS) - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(
        SERVICE_NAME,
        "batch_complete",
        topic="shopflow.products.updated",
        published=published,
    )

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic="shopflow.products.updated", published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
