"""Publishes shipment status transitions for the ShopFlow carrier network.

A short burst of scan events is emitted on startup and the process then stays
resident so the platform health probe keeps passing.
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

from confluent_kafka import KafkaException, Message, Producer

import shipment_pb2

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "logistics-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

TOPIC: Final[str] = "shopflow.logistics.shipment_updated"

EVENT_COUNT: Final[int] = 5
EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

SCAN_LOG: Final[tuple[tuple[str, str, str, int], ...]] = (
    (
        "SHP-100341",
        "DC-ATL-01",
        "STORE-1145",
        shipment_pb2.ShipmentUpdate.PENDING,
    ),
    (
        "SHP-100342",
        "DC-ATL-01",
        "STORE-2210",
        shipment_pb2.ShipmentUpdate.IN_TRANSIT,
    ),
    (
        "SHP-100343",
        "DC-DFW-04",
        "STORE-0388",
        shipment_pb2.ShipmentUpdate.IN_TRANSIT,
    ),
    (
        "SHP-100344",
        "DC-SEA-02",
        "STORE-4471",
        shipment_pb2.ShipmentUpdate.DELIVERED,
    ),
    (
        "SHP-100345",
        "DC-DFW-04",
        "STORE-1145",
        shipment_pb2.ShipmentUpdate.PENDING,
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


def build_updates(count: int = EVENT_COUNT) -> list[shipment_pb2.ShipmentUpdate]:
    scanned_at = int(time.time())
    updates: list[shipment_pb2.ShipmentUpdate] = []
    for index in range(count):
        shipment_id, origin, destination, status = SCAN_LOG[index % len(SCAN_LOG)]
        updates.append(
            shipment_pb2.ShipmentUpdate(
                shipment_id=shipment_id,
                origin=origin,
                destination=destination,
                status=status,
                updated_at=scanned_at + int(index * EVENT_INTERVAL_SECONDS),
            )
        )
    return updates


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


def publish(producer: Producer, update: shipment_pb2.ShipmentUpdate) -> None:
    producer.produce(
        topic=TOPIC,
        key=update.shipment_id.encode("utf-8"),
        value=update.SerializeToString(),
        headers=[
            ("content-type", b"application/x-protobuf"),
            ("proto-message", b"ShipmentUpdate"),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic=TOPIC,
        message_id=update.shipment_id,
        origin=update.origin,
        destination=update.destination,
        status=shipment_pb2.ShipmentUpdate.Status.Name(update.status),
        payload_bytes=update.ByteSize(),
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
    for index, update in enumerate(build_updates()):
        if _shutdown.is_set():
            break
        publish(producer, update)
        published += 1
        if index < EVENT_COUNT - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", topic=TOPIC, published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=TOPIC, published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
