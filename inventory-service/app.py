"""Inventory service.

Consumes ``OrderPlacedEvent`` from the order stream, draws down on-hand stock
for each line, and republishes an ``InventoryAdjustedEvent`` per product so the
storefront and procurement systems converge. On-hand counts live in an
in-process dictionary here; the production deployment writes through to Postgres.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from pydantic import ValidationError

from models import InventoryAdjustedEvent, OrderPlacedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "inventory-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP: Final[str] = os.environ.get("KAFKA_CONSUMER_GROUP", "inventory-service")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

ORDERS_TOPIC: Final[str] = "shopflow.orders.placed"
INVENTORY_TOPIC: Final[str] = "shopflow.inventory.adjusted"

POLL_TIMEOUT_SECONDS: Final[float] = 1.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

INITIAL_ON_HAND: Final[int] = 100

_shutdown = threading.Event()
_on_hand: dict[str, int] = defaultdict(lambda: INITIAL_ON_HAND)


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
    client: Consumer | Producer,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the broker answers a metadata request, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while not _shutdown.is_set():
        attempt += 1
        try:
            client.list_topics(timeout=5.0)
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


def header_value(msg: Message, key: str) -> str | None:
    for name, value in msg.headers() or []:
        if name == key:
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None


def apply_adjustment(product_id: str, quantity: int) -> int:
    """Draw down on-hand stock for a product and return the new on-hand count."""
    _on_hand[product_id] = max(0, _on_hand[product_id] - quantity)
    return _on_hand[product_id]


def build_adjustments(event: OrderPlacedEvent) -> list[InventoryAdjustedEvent]:
    """Produce one adjustment per line in the order."""
    adjustments: list[InventoryAdjustedEvent] = []
    for line in event.lines:
        on_hand = apply_adjustment(line.product_id, line.quantity)
        adjustments.append(
            InventoryAdjustedEvent(
                product_id=line.product_id,
                order_id=event.order_id,
                delta=-line.quantity,
                on_hand=on_hand,
            )
        )
    return adjustments


def publish_adjustment(producer: Producer, adjustment: InventoryAdjustedEvent) -> None:
    producer.produce(
        topic=INVENTORY_TOPIC,
        key=adjustment.product_id.encode("utf-8"),
        value=adjustment.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", b"inventory.adjusted"),
            ("message-id", str(uuid4()).encode("utf-8")),
        ],
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "adjusted",
        topic=INVENTORY_TOPIC,
        product_id=adjustment.product_id,
        order_id=adjustment.order_id,
        delta=adjustment.delta,
        on_hand=adjustment.on_hand,
    )


def handle(msg: Message, producer: Producer) -> None:
    raw = msg.value()
    if raw is None:
        log(SERVICE_NAME, "empty_message", topic=msg.topic(), offset=msg.offset())
        return
    try:
        event = OrderPlacedEvent.model_validate_json(raw)
    except ValidationError as exc:
        log(
            SERVICE_NAME,
            "invalid_payload",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=exc.errors(include_url=False),
        )
        return

    for adjustment in build_adjustments(event):
        publish_adjustment(producer, adjustment)

    log(
        SERVICE_NAME,
        "consumed",
        topic=msg.topic(),
        message_id=header_value(msg, "message-id"),
        order_id=event.order_id,
        lines=len(event.lines),
        partition=msg.partition(),
        offset=msg.offset(),
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        consumes=ORDERS_TOPIC,
        produces=INVENTORY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group=CONSUMER_GROUP,
        health_port=HEALTH_PORT,
    )

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": f"{SERVICE_NAME}-producer-0",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": CONSUMER_GROUP,
            "client.id": f"{SERVICE_NAME}-0",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
        }
    )
    wait_for_broker(consumer)
    consumer.subscribe([ORDERS_TOPIC])
    log(SERVICE_NAME, "subscribed", topic=ORDERS_TOPIC, consumer_group=CONSUMER_GROUP)

    consumed = 0
    try:
        while not _shutdown.is_set():
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            error = msg.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                log(SERVICE_NAME, "consume_error", topic=msg.topic(), error=str(error))
                continue
            handle(msg, producer)
            consumer.commit(message=msg, asynchronous=False)
            consumed += 1
    finally:
        log(SERVICE_NAME, "stopping", consumed=consumed)
        producer.flush(10.0)
        consumer.close()


if __name__ == "__main__":
    main()
