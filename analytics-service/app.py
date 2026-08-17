"""Streams ShopFlow storefront product views to Kafka.

A short burst of sample views is emitted on startup and the process then stays
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

from confluent_kafka import KafkaException, Producer

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "analytics-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

EVENT_COUNT: Final[int] = 5
EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

STOREFRONT_VIEWS: Final[tuple[tuple[str, str, str], ...]] = (
    ("PRD-88213", "USR-40217", "sess-9c1f4a2b"),
    ("PRD-11907", "USR-51884", "sess-3d70e5c8"),
    ("PRD-88213", "USR-51884", "sess-3d70e5c8"),
    ("PRD-64530", "USR-27356", "sess-be08117d"),
    ("PRD-11907", "USR-40217", "sess-9c1f4a2b"),
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


def track_product_view(
    producer: Producer,
    product_id: str,
    user_id: str,
    session_id: str,
) -> None:
    producer.produce("shopflow.analytics.product_viewed", value=json.dumps({
        "event": "product_viewed",
        "product_id": product_id,
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "session_id": session_id,
    }).encode())
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic="shopflow.analytics.product_viewed",
        message_id=session_id,
        product_id=product_id,
        user_id=user_id,
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
    for index in range(EVENT_COUNT):
        if _shutdown.is_set():
            break
        product_id, user_id, session_id = STOREFRONT_VIEWS[index % len(STOREFRONT_VIEWS)]
        track_product_view(producer, product_id, user_id, session_id)
        published += 1
        if index < EVENT_COUNT - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(
        SERVICE_NAME,
        "batch_complete",
        published=published,
        undelivered=undelivered,
    )

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
