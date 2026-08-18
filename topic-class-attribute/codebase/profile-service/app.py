"""Profile service.

Materialises the customer profile projection from identity events. The
projection is held in memory here; the production deployment writes it to
DynamoDB.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Final

from confluent_kafka import KafkaError, KafkaException, Message
from pydantic import ValidationError

from consumer import UserEventConsumer
from models import UserRegisteredEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "profile-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP: Final[str] = os.environ.get("KAFKA_CONSUMER_GROUP", "profile-service")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

POLL_TIMEOUT_SECONDS: Final[float] = 1.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

_shutdown = threading.Event()
_profiles: dict[str, dict[str, Any]] = {}


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
    consumer: UserEventConsumer,
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


def header_value(msg: Message, key: str) -> str | None:
    for name, value in msg.headers() or []:
        if name == key:
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None


def handle(msg: Message) -> None:
    raw = msg.value()
    if raw is None:
        log(SERVICE_NAME, "empty_message", topic=msg.topic(), offset=msg.offset())
        return
    try:
        event = UserRegisteredEvent.model_validate_json(raw)
    except ValidationError as exc:
        log(
            SERVICE_NAME,
            "invalid_payload",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=exc.errors(include_url=False),
        )
        return

    _profiles[event.user_id] = {
        "user_id": event.user_id,
        "display_name": event.name,
        "email": event.email,
        "member_since": event.registered_at,
    }
    log(
        SERVICE_NAME,
        "consumed",
        topic=msg.topic(),
        message_id=header_value(msg, "message-id"),
        user_id=event.user_id,
        partition=msg.partition(),
        offset=msg.offset(),
        profiles=len(_profiles),
    )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)

    consumer = UserEventConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group=CONSUMER_GROUP,
        client_id=f"{SERVICE_NAME}-0",
    )
    log(
        SERVICE_NAME,
        "started",
        topic=consumer.topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group=CONSUMER_GROUP,
        health_port=HEALTH_PORT,
    )
    wait_for_broker(consumer)
    consumer.subscribe()
    log(SERVICE_NAME, "subscribed", topic=consumer.topic, consumer_group=CONSUMER_GROUP)

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
                log(SERVICE_NAME, "consume_error", topic=msg.topic(), error=str(error))
                continue
            handle(msg)
            consumer.commit(msg)
            consumed += 1
    finally:
        log(
            SERVICE_NAME,
            "stopping",
            topic=consumer.topic,
            consumed=consumed,
            profiles=len(_profiles),
        )
        consumer.close()


if __name__ == "__main__":
    main()
