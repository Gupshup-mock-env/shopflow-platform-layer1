"""Catalogue enrichment worker.

Takes raw product records from the merchandising snapshot, derives the
presentation attributes the storefront needs and publishes the enriched
record. Runs a short burst on startup and then stays resident so the
platform health probe keeps passing.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from uuid import uuid4

from confluent_kafka import KafkaException, Message, Producer

SERVICE_NAME = os.environ.get("SERVICE_NAME", "enrichment-service")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))

BROKER_WAIT_SECONDS = 60.0
PUBLISH_INTERVAL_SECONDS = 2.0
FLUSH_TIMEOUT_SECONDS = 10.0

SNAPSHOT: tuple[dict[str, Any], ...] = (
    {
        "id": "PRD-10421",
        "title": "Aurora Pour-Over Kettle",
        "brand": "Aurora",
        "weight_grams": 850,
        "color": "graphite",
    },
    {
        "id": "PRD-10422",
        "title": "Trailhead 30L Daypack",
        "brand": "Trailhead",
        "weight_grams": 1240,
        "color": "moss",
    },
    {
        "id": "PRD-10423",
        "title": "Nimbus Desk Lamp",
        "brand": "Nimbus",
        "weight_grams": 640,
    },
    {
        "id": "PRD-10424",
        "title": "Harbor Wool Throw",
        "color": "oatmeal",
    },
    {
        "id": "PRD-10425",
        "title": "Cobalt Cycling Bottle",
        "brand": "Cobalt",
        "weight_grams": 210,
        "color": "cobalt",
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


def build_attributes(product: dict[str, Any]) -> dict[str, Any]:
    return {
        "brand": product.get("brand", "unknown"),
        "weight_grams": product.get("weight_grams", 0),
        "color": product.get("color", "unspecified"),
    }


def publish_enriched(product: dict[str, Any], producer: Producer) -> str:
    message_id = str(uuid4())
    event: dict[str, Any] = {}
    event["type"] = "product_enriched"
    event["product_id"] = product["id"]
    event["attributes"] = build_attributes(product)
    event["enriched_at"] = datetime.now(timezone.utc).isoformat()

    producer.produce(
        "shopflow.catalog.enriched",
        key=product["id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic="shopflow.catalog.enriched",
        message_id=message_id,
        product_id=event["product_id"],
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
        snapshot_size=len(SNAPSHOT),
    )

    producer = build_producer()
    wait_for_broker(producer)

    published = 0
    for index, product in enumerate(SNAPSHOT):
        if _shutdown.is_set():
            break
        publish_enriched(product, producer)
        published += 1
        if index < len(SNAPSHOT) - 1:
            _shutdown.wait(PUBLISH_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", published=published)

    _shutdown.wait()

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "stopping", published=published)


if __name__ == "__main__":
    main()
