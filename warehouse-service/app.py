"""Warehouse stock projector.

Applies inventory stock movements to the per-warehouse on-hand counts and
commits the offset once each record has been handled.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from config import AppConfig, ConsumerConfig, load_config
from models import StockUpdatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "warehouse-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

CONFIG: Final[AppConfig] = load_config()
TOPIC: Final[str] = CONFIG.topic("stock_updated")
CONSUMER: Final[ConsumerConfig] = CONFIG.consumer("warehouse-service")
HEALTH_PORT: Final[int] = CONFIG.health_port

BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

_shutdown = threading.Event()
_on_hand: dict[tuple[str, str], int] = defaultdict(int)


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


def message_id_of(msg: Message) -> str:
    for key, value in msg.headers() or []:
        if key == "message-id" and value is not None:
            return value.decode("utf-8")
    return f"{msg.topic()}-{msg.partition()}-{msg.offset()}"


def handle(msg: Message) -> None:
    raw = msg.value()
    if raw is None:
        log(SERVICE_NAME, "empty_message", topic=msg.topic(), offset=msg.offset())
        return
    try:
        event = StockUpdatedEvent.model_validate_json(raw)
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

    position = (event.warehouse_id, event.sku)
    _on_hand[position] += event.quantity_delta
    log(
        SERVICE_NAME,
        "consumed",
        topic=msg.topic(),
        message_id=message_id_of(msg),
        sku=event.sku,
        warehouse_id=event.warehouse_id,
        quantity_delta=event.quantity_delta,
        reason=event.reason,
        on_hand=_on_hand[position],
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
        consumer_group=CONSUMER.group_id,
        config_path=str(CONFIG.path),
        health_port=HEALTH_PORT,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": CONSUMER.group_id,
            "client.id": f"{SERVICE_NAME}-0",
            "auto.offset.reset": CONSUMER.auto_offset_reset,
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
        }
    )
    wait_for_broker(consumer)
    consumer.subscribe([TOPIC])
    log(SERVICE_NAME, "subscribed", topic=TOPIC, consumer_group=CONSUMER.group_id)

    consumed = 0
    try:
        while not _shutdown.is_set():
            msg = consumer.poll(CONSUMER.poll_timeout_seconds)
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
