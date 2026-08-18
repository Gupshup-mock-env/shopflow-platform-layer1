"""ShopFlow payment service.

Authorises payment for every order the platform accepts, then commits the
offset so the authorisation is not replayed on restart.
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

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from models import OrderCreatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "payment-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

POLL_TIMEOUT_SECONDS: Final[float] = 1.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
AUTHORISATION_CEILING_CENTS: Final[int] = 250_000

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


def header_value(msg: Message, name: str) -> str | None:
    for key, value in msg.headers() or []:
        if key != name:
            continue
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value
    return None


def authorise(event: OrderCreatedEvent) -> tuple[str, str]:
    """Return an authorisation reference and its outcome for an order."""
    if event.total_cents > AUTHORISATION_CEILING_CENTS:
        return "", "referred"
    return f"AUTH-{uuid4().hex[:12].upper()}", "authorised"


def handle(msg: Message) -> None:
    raw = msg.value()
    if raw is None:
        log(SERVICE_NAME, "empty_message", topic=msg.topic(), offset=msg.offset())
        return

    message_id = header_value(msg, "message-id")
    try:
        event = OrderCreatedEvent.model_validate_json(raw)
    except ValidationError as exc:
        log(
            SERVICE_NAME,
            "invalid_payload",
            topic=msg.topic(),
            message_id=message_id,
            partition=msg.partition(),
            offset=msg.offset(),
            error=exc.errors(include_url=False),
        )
        return

    log(
        SERVICE_NAME,
        "consumed",
        topic=msg.topic(),
        message_id=message_id,
        order_id=event.order_id,
        customer_id=event.customer_id,
        total_cents=event.total_cents,
        currency=event.currency,
        item_count=len(event.items),
        partition=msg.partition(),
        offset=msg.offset(),
    )

    authorisation_id, outcome = authorise(event)
    log(
        SERVICE_NAME,
        "payment_authorisation",
        topic=msg.topic(),
        message_id=message_id,
        order_id=event.order_id,
        authorisation_id=authorisation_id,
        outcome=outcome,
        amount_cents=event.total_cents,
        currency=event.currency,
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

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "payment-service",
            "client.id": f"{SERVICE_NAME}-0",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        }
    )
    wait_for_broker(consumer)
    consumer.subscribe(["shopflow.orders.created"])
    log(
        SERVICE_NAME,
        "subscribed",
        topic="shopflow.orders.created",
        consumer_group="payment-service",
    )

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
                log(SERVICE_NAME, "consume_error", error=str(error))
                continue
            handle(msg)
            consumer.commit(message=msg, asynchronous=False)
            consumed += 1
    finally:
        log(SERVICE_NAME, "stopping", consumed=consumed)
        consumer.close()


if __name__ == "__main__":
    main()
