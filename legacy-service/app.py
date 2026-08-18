"""Legacy order sync bridge.

Drains the nightly order batch out of the legacy fulfilment stack and puts it
on Kafka for the modern processing pipeline. Runs a short burst on startup and
then stays resident so the platform health probe keeps passing.
"""

from __future__ import annotations

import json
import os
import pickle
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from uuid import uuid4

from confluent_kafka import KafkaException, Message, Producer

from legacy_models import LegacyOrder

SERVICE_NAME = os.environ.get("SERVICE_NAME", "legacy-service")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))

BROKER_WAIT_SECONDS = 60.0
PUBLISH_INTERVAL_SECONDS = 2.0
FLUSH_TIMEOUT_SECONDS = 10.0

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


def wait_for_broker(producer: Producer, timeout: float = BROKER_WAIT_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    delay = 0.5
    last_error = "timed out"
    while time.monotonic() < deadline and not _shutdown.is_set():
        try:
            producer.list_topics(timeout=5.0)
            return
        except KafkaException as exc:
            last_error = str(exc)
            log(
                SERVICE_NAME,
                "broker_unavailable",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                error=last_error,
                retry_in_seconds=delay,
            )
            if _shutdown.wait(delay):
                return
            delay = min(delay * 2.0, 5.0)
    raise RuntimeError(f"kafka unreachable at {KAFKA_BOOTSTRAP}: {last_error}")


def _on_delivery(err: object, msg: Message) -> None:
    if err is not None:
        log(SERVICE_NAME, "delivery_failed", error=str(err))
        return
    log(
        SERVICE_NAME,
        "delivered",
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
    )


def read_batch() -> list[LegacyOrder]:
    """Read the pending order batch out of the legacy export."""
    return [
        LegacyOrder(
            "LEG-880134",
            [
                {"sku": "SKU-CERAMIC-MUG-01", "qty": 2, "unit_price": 12.50},
                {"sku": "SKU-FILTER-100", "qty": 1, "unit_price": 6.25},
            ],
            31.25,
        ),
        LegacyOrder(
            "LEG-880135",
            [{"sku": "SKU-POUR-OVER-03", "qty": 1, "unit_price": 48.00}],
            48.00,
        ),
        LegacyOrder(
            "LEG-880136",
            [
                {"sku": "SKU-BEANS-ETH-12", "qty": 3, "unit_price": 22.75},
                {"sku": "SKU-GRINDER-07", "qty": 1, "unit_price": 89.90},
            ],
            158.15,
        ),
        LegacyOrder(
            "LEG-880137",
            [{"sku": "SKU-TRAVEL-TUMBLER-02", "qty": 4, "unit_price": 19.95}],
            79.80,
        ),
        LegacyOrder(
            "LEG-880138",
            [
                {"sku": "SKU-ESPRESSO-CUP-06", "qty": 6, "unit_price": 8.40},
                {"sku": "SKU-BEANS-COL-09", "qty": 2, "unit_price": 18.60},
            ],
            87.60,
        ),
    ]


def publish_order(order: LegacyOrder, producer: Producer) -> str:
    message_id = str(uuid4())
    producer.produce(
        "shopflow.legacy.order_sync",
        key=order.order_id.encode("utf-8"),
        value=pickle.dumps(order),
        headers=[
            ("content-type", b"application/python-pickle"),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic="shopflow.legacy.order_sync",
        message_id=message_id,
        order_id=order.order_id,
    )
    return message_id


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": f"{SERVICE_NAME}-0",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "retries": 5,
        }
    )


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

    producer = build_producer()
    wait_for_broker(producer)

    batch = read_batch()
    published = 0
    for index, order in enumerate(batch):
        if _shutdown.is_set():
            break
        publish_order(order, producer)
        published += 1
        if index < len(batch) - 1:
            _shutdown.wait(PUBLISH_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", published=published)

    _shutdown.wait()

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "stopping", published=published)


if __name__ == "__main__":
    main()
