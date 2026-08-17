"""Aggregates ShopFlow storefront product views.

Reads view events off Kafka, folds them into per-product and per-session
counters and commits the offset once the event has been counted.
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

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "collector-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP: Final[str] = os.environ.get("KAFKA_CONSUMER_GROUP", "collector-service")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

TOPIC: Final[str] = "shopflow.analytics.product_viewed"

POLL_TIMEOUT_SECONDS: Final[float] = 1.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

_shutdown = threading.Event()
_views_by_product: dict[str, int] = {}
_sessions_by_product: dict[str, set[str]] = {}


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
    consumer: Consumer,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the broker answers a metadata request, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while not _shutdown.is_set():
        attempt += 1
        try:
            consumer.list_topics(timeout=5.0)
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


def count_view(product_id: str, session_id: str) -> tuple[int, int]:
    """Fold one view into the counters and return (views, unique sessions)."""
    _views_by_product[product_id] = _views_by_product.get(product_id, 0) + 1
    _sessions_by_product.setdefault(product_id, set()).add(session_id)
    return _views_by_product[product_id], len(_sessions_by_product[product_id])


def handle(msg: Message) -> None:
    raw = msg.value()
    if raw is None:
        log(SERVICE_NAME, "empty_message", topic=msg.topic(), offset=msg.offset())
        return
    try:
        view = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log(
            SERVICE_NAME,
            "invalid_payload",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=str(exc),
        )
        return

    if view.get("event") != "product_viewed":
        log(
            SERVICE_NAME,
            "skipped",
            topic=msg.topic(),
            offset=msg.offset(),
            event_type=view.get("event"),
        )
        return

    views, unique_sessions = count_view(view["product_id"], view["session_id"])
    log(
        SERVICE_NAME,
        "consumed",
        topic=msg.topic(),
        message_id=view["session_id"],
        event_type=view["event"],
        product_id=view["product_id"],
        user_id=view["user_id"],
        viewed_at=view["timestamp"],
        product_views=views,
        unique_sessions=unique_sessions,
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
        topic=TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group=CONSUMER_GROUP,
        health_port=HEALTH_PORT,
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
    consumer.subscribe([TOPIC])
    log(SERVICE_NAME, "subscribed", topic=TOPIC, consumer_group=CONSUMER_GROUP)

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
                log(SERVICE_NAME, "consume_error", topic=TOPIC, error=str(error))
                continue
            handle(msg)
            consumer.commit(message=msg, asynchronous=False)
            consumed += 1
    finally:
        log(SERVICE_NAME, "stopping", topic=TOPIC, consumed=consumed)
        consumer.close()


if __name__ == "__main__":
    main()
