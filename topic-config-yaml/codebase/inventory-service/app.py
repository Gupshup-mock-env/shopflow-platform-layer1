"""Inventory ledger publisher.

Emits a short burst of stock movements on startup and then stays resident so
the platform health probe keeps passing.
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

from confluent_kafka import KafkaException, Message, Producer

from config import AppConfig, load_config
from models import StockUpdatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "inventory-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

CONFIG: Final[AppConfig] = load_config()
TOPIC: Final[str] = CONFIG.topic("stock_updated")
HEALTH_PORT: Final[int] = CONFIG.health_port

EVENT_INTERVAL_SECONDS: Final[float] = CONFIG.publish_interval_seconds
FLUSH_TIMEOUT_SECONDS: Final[float] = CONFIG.delivery_timeout_seconds
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

MOVEMENTS: Final[tuple[tuple[str, str, int, str], ...]] = (
    ("SKU-CERAMIC-MUG-01", "WH-EAST-01", -12, "order_allocated"),
    ("SKU-POUR-OVER-03", "WH-EAST-01", 48, "purchase_order_received"),
    ("SKU-BEANS-ETH-12", "WH-WEST-03", -3, "damaged_in_transit"),
    ("SKU-GRINDER-07", "WH-WEST-03", -25, "order_allocated"),
    ("SKU-FILTER-100", "WH-CENTRAL-02", 120, "cycle_count_adjustment"),
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


def build_sample_events() -> list[StockUpdatedEvent]:
    return [
        StockUpdatedEvent(
            sku=sku,
            warehouse_id=warehouse_id,
            quantity_delta=quantity_delta,
            reason=reason,
        )
        for sku, warehouse_id, quantity_delta, reason in MOVEMENTS
    ]


def _on_delivery(err: object, msg: Message) -> None:
    if err is not None:
        log(SERVICE_NAME, "delivery_failed", topic=TOPIC, error=str(err))
        return
    log(
        SERVICE_NAME,
        "delivered",
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
    )


def publish(producer: Producer, event: StockUpdatedEvent) -> None:
    message_id = f"stk-{uuid4().hex[:12]}"
    producer.produce(
        topic=TOPIC,
        key=event.sku.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", b"inventory.stock_updated"),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic=TOPIC,
        message_id=message_id,
        sku=event.sku,
        warehouse_id=event.warehouse_id,
        quantity_delta=event.quantity_delta,
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic=TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        config_path=str(CONFIG.path),
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

    events = build_sample_events()
    published = 0
    for index, event in enumerate(events):
        if _shutdown.is_set():
            break
        publish(producer, event)
        published += 1
        if index < len(events) - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", topic=TOPIC, published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=TOPIC, published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
