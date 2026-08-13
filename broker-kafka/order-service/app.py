"""ShopFlow order service.

Publishes an event for every order checkout accepts. A short burst of sample
orders is emitted on startup, after which the process stays resident so the
platform health probe keeps passing.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final
from uuid import uuid4

from confluent_kafka import KafkaException, Message, Producer

from models import OrderCreatedEvent, OrderItem

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "order-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

ORDER_COUNT: Final[int] = 5
ORDER_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

BASKETS: Final[tuple[tuple[str, tuple[tuple[str, int, int], ...]], ...]] = (
    (
        "CUST-10428",
        (("SKU-ESPRESSO-CUP-02", 4, 1150), ("SKU-SAUCER-02", 4, 725)),
    ),
    (
        "CUST-10593",
        (("SKU-POUR-OVER-KETTLE", 1, 8990),),
    ),
    (
        "CUST-11004",
        (("SKU-BEANS-ETH-1KG", 2, 2275), ("SKU-FILTER-100", 3, 625)),
    ),
    (
        "CUST-10871",
        (("SKU-HAND-GRINDER-07", 1, 7450), ("SKU-SCALE-DIGITAL", 1, 4299)),
    ),
    (
        "CUST-11236",
        (
            ("SKU-COLD-BREW-JUG", 1, 3150),
            ("SKU-BEANS-COL-500G", 2, 1495),
            ("SKU-CLEAN-TABS-24", 1, 899),
        ),
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


def build_sample_orders(count: int = ORDER_COUNT) -> list[OrderCreatedEvent]:
    orders: list[OrderCreatedEvent] = []
    for index in range(count):
        customer_id, basket = BASKETS[index % len(BASKETS)]
        items = [
            OrderItem(sku=sku, quantity=quantity, price_cents=price_cents)
            for sku, quantity, price_cents in basket
        ]
        orders.append(
            OrderCreatedEvent(
                order_id=f"ORD-{903150 + index}",
                customer_id=customer_id,
                total_cents=sum(item.quantity * item.price_cents for item in items),
                currency="USD",
                items=items,
            )
        )
    return orders


def _on_delivery(message_id: str, err: object, msg: Message) -> None:
    if err is not None:
        log(
            SERVICE_NAME,
            "delivery_failed",
            topic=msg.topic(),
            message_id=message_id,
            error=str(err),
        )
        return
    log(
        SERVICE_NAME,
        "delivered",
        topic=msg.topic(),
        message_id=message_id,
        partition=msg.partition(),
        offset=msg.offset(),
    )


def publish_order_created(producer: Producer, order: OrderCreatedEvent) -> str:
    message_id = str(uuid4())
    producer.produce(
        "shopflow.orders.created",
        key=order.order_id.encode("utf-8"),
        value=order.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", b"order.created"),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=partial(_on_delivery, message_id),
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic="shopflow.orders.created",
        message_id=message_id,
        order_id=order.order_id,
        customer_id=order.customer_id,
        total_cents=order.total_cents,
        item_count=len(order.items),
    )
    return message_id


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
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
            "compression.type": "snappy",
        }
    )
    wait_for_broker(producer)

    published = 0
    orders = build_sample_orders()
    for index, order in enumerate(orders):
        if _shutdown.is_set():
            break
        publish_order_created(producer, order)
        published += 1
        if index < len(orders) - 1:
            _shutdown.wait(ORDER_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
