"""Storefront record service.

Consumes enriched catalogue records and keeps the storefront product index up
to date. The index is an in-process dictionary here; the production
deployment writes through to the storefront datastore.
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

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka.admin import AdminClient

SERVICE_NAME = os.environ.get("SERVICE_NAME", "store-service")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "store-service")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))

BROKER_WAIT_SECONDS = 60.0
POLL_TIMEOUT_SECONDS = 1.0

_shutdown = threading.Event()
_records: dict[str, dict[str, Any]] = {}


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


def wait_for_broker(bootstrap: str, timeout: float = BROKER_WAIT_SECONDS) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout
    delay = 0.5
    last_error = "timed out"
    while time.monotonic() < deadline and not _shutdown.is_set():
        try:
            admin.list_topics(timeout=5.0)
            return
        except KafkaException as exc:
            last_error = str(exc)
            log(SERVICE_NAME, "broker_unavailable", error=last_error, retry_in_seconds=delay)
            if _shutdown.wait(delay):
                return
            delay = min(delay * 2.0, 5.0)
    raise RuntimeError(f"kafka unreachable at {bootstrap}: {last_error}")


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_CONSUMER_GROUP,
            "client.id": f"{SERVICE_NAME}-0",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
        }
    )


def header_value(msg: Message, key: str) -> str | None:
    for name, value in msg.headers() or []:
        if name == key:
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None


def upsert_record(product_id: str, attributes: dict[str, Any], enriched_at: str) -> None:
    _records[product_id] = {
        "brand": attributes.get("brand", "unknown"),
        "weight_grams": attributes.get("weight_grams", 0),
        "color": attributes.get("color", "unspecified"),
        "enriched_at": enriched_at,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
    }


def handle_message(msg: Message) -> None:
    message_id = header_value(msg, "message-id")
    raw = msg.value()
    if raw is None:
        log(SERVICE_NAME, "empty_message", topic=msg.topic(), message_id=message_id)
        return
    try:
        event = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log(
            SERVICE_NAME,
            "invalid_payload",
            topic=msg.topic(),
            message_id=message_id,
            error=str(exc),
        )
        return

    if event.get("type") != "product_enriched":
        log(
            SERVICE_NAME,
            "ignored_event",
            topic=msg.topic(),
            message_id=message_id,
            event_type=event.get("type"),
        )
        return

    product_id = event["product_id"]
    attributes = event.get("attributes") or {}
    upsert_record(product_id, attributes, event["enriched_at"])
    log(
        SERVICE_NAME,
        "consumed",
        topic=msg.topic(),
        message_id=message_id,
        product_id=product_id,
        brand=attributes.get("brand"),
        partition=msg.partition(),
        offset=msg.offset(),
        record_count=len(_records),
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_CONSUMER_GROUP,
        health_port=HEALTH_PORT,
    )

    wait_for_broker(KAFKA_BOOTSTRAP)

    consumer = build_consumer()
    consumer.subscribe(["shopflow.catalog.enriched"])

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log(SERVICE_NAME, "consume_error", error=str(msg.error()))
                continue

            handle_message(msg)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        log(SERVICE_NAME, "stopping", record_count=len(_records))


if __name__ == "__main__":
    main()
