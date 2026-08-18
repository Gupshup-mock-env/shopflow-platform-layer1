"""Returns authorisation publisher.

Opens a short burst of sample return authorisations on startup and then stays
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
from uuid import uuid4

from confluent_kafka import KafkaException, Message, Producer
from dotenv import find_dotenv, load_dotenv

from models import ReturnInitiatedEvent

ENV_FILE: Final[str] = find_dotenv(usecwd=True)
load_dotenv(ENV_FILE, override=False)

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "returns-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

TOPIC: Final[str] = os.environ["RETURNS_TOPIC"]

EVENT_INTERVAL_SECONDS: Final[float] = float(os.environ.get("PUBLISH_INTERVAL_SECONDS", "2"))
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

AUTHORISATIONS: Final[tuple[tuple[str, str, str, tuple[str, ...]], ...]] = (
    ("RET-100341", "ORD-889201", "wrong_size", ("SKU-JACKET-M", "SKU-BELT-L")),
    ("RET-100342", "ORD-889244", "damaged_on_arrival", ("SKU-POUR-OVER-03",)),
    ("RET-100343", "ORD-889310", "changed_mind", ("SKU-CERAMIC-MUG-01", "SKU-FILTER-100")),
    ("RET-100344", "ORD-889377", "not_as_described", ("SKU-LAMP-BRASS",)),
    ("RET-100345", "ORD-889402", "late_delivery", ("SKU-BEANS-ETH-12", "SKU-GRINDER-07")),
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


def build_sample_events() -> list[ReturnInitiatedEvent]:
    return [
        ReturnInitiatedEvent(
            return_id=return_id,
            order_id=order_id,
            reason=reason,
            items=list(items),
        )
        for return_id, order_id, reason, items in AUTHORISATIONS
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


def publish(producer: Producer, event: ReturnInitiatedEvent) -> None:
    message_id = f"ret-{uuid4().hex[:12]}"
    producer.produce(
        topic=TOPIC,
        key=event.return_id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
        headers=[
            ("content-type", b"application/json"),
            ("event-type", b"returns.initiated"),
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
        return_id=event.return_id,
        order_id=event.order_id,
        item_count=len(event.items),
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
        env_file=ENV_FILE,
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
