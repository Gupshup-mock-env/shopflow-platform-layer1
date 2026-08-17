"""ShopFlow analytics collector.

Fans client-side interactions out to one Kafka stream per region and event
type, then stays resident so the platform health probe keeps passing.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final
from uuid import uuid4

from confluent_kafka import KafkaException, Message, Producer

from models import AnalyticsEvent
from topics import current_region, topic_for

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "analytics-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

REGION: Final[str] = current_region()

EVENT_COUNT: Final[int] = 5
EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

SESSION_ID: Final[str] = "sess-9f4c21ab"

INTERACTIONS: Final[tuple[tuple[str, str, dict], ...]] = (
    (
        "page_view",
        "USR-40218",
        {"path": "/p/ceramic-mug", "referrer": "google", "device": "desktop"},
    ),
    (
        "click",
        "USR-40218",
        {"element": "add-to-cart", "sku": "SKU-CERAMIC-MUG-01", "position": 1},
    ),
    (
        "purchase",
        "USR-40218",
        {"order_id": "ORD-51902", "total_cents": 2500, "currency": "USD"},
    ),
    (
        "page_view",
        "USR-40218",
        {"path": "/orders/ORD-51902", "referrer": "internal", "device": "desktop"},
    ),
    (
        "click",
        "USR-40218",
        {"element": "recommendation-tile", "sku": "SKU-BEANS-ETH-12", "position": 3},
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


def build_sample_events(count: int = EVENT_COUNT) -> list[AnalyticsEvent]:
    occurred_at = datetime.now(timezone.utc)
    events: list[AnalyticsEvent] = []
    for index in range(count):
        event_type, user_id, properties = INTERACTIONS[index % len(INTERACTIONS)]
        events.append(
            AnalyticsEvent(
                event_type=event_type,
                user_id=user_id,
                session_id=SESSION_ID,
                timestamp=(
                    occurred_at + timedelta(seconds=index * EVENT_INTERVAL_SECONDS)
                ).isoformat(),
                properties=properties,
            )
        )
    return events


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


def publish(producer: Producer, event: AnalyticsEvent) -> None:
    topic = topic_for(REGION, event.event_type)
    message_id = str(uuid4())
    producer.produce(
        topic=topic,
        key=event.session_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", event.event_type.encode("utf-8")),
            ("region", REGION.encode("utf-8")),
            ("message-id", message_id.encode("utf-8")),
        ],
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    log(
        SERVICE_NAME,
        "published",
        topic=topic,
        message_id=message_id,
        event_type=event.event_type,
        user_id=event.user_id,
        session_id=event.session_id,
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        region=REGION,
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
    for index, event in enumerate(build_sample_events()):
        if _shutdown.is_set():
            break
        publish(producer, event)
        published += 1
        if index < EVENT_COUNT - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    producer.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", region=REGION, published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", region=REGION, published=published)
    producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
