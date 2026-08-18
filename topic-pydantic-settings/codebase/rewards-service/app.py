"""Rewards service: projects loyalty point awards into redeemable balances."""

from __future__ import annotations

import json
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from events import PointsEarnedEvent
from settings import Settings, get_settings

_shutdown = threading.Event()
_balances: dict[str, int] = defaultdict(int)


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


def wait_for_broker(consumer: Consumer, settings: Settings, timeout_seconds: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    delay = 0.5
    last_error = "unknown"
    while time.monotonic() < deadline and not _shutdown.is_set():
        try:
            consumer.list_topics(timeout=5.0)
            return
        except KafkaException as exc:
            last_error = str(exc)
            log(
                settings.service_name,
                "broker_unavailable",
                bootstrap_servers=settings.kafka_bootstrap,
                retry_in_seconds=delay,
                error=last_error,
            )
            if _shutdown.wait(delay):
                return
            delay = min(delay * 2, 5.0)
    raise RuntimeError(f"kafka not reachable at {settings.kafka_bootstrap}: {last_error}")


def message_id_of(message: Message) -> str:
    for key, value in message.headers() or []:
        if key == "message_id" and value is not None:
            return value.decode("utf-8")
    return f"{message.topic()}-{message.partition()}-{message.offset()}"


def handle(message: Message, settings: Settings) -> None:
    message_id = message_id_of(message)
    try:
        event = PointsEarnedEvent.model_validate_json(message.value() or b"")
    except ValidationError as exc:
        log(
            settings.service_name,
            "invalid_payload",
            topic=settings.points_topic,
            message_id=message_id,
            errors=exc.error_count(),
        )
        return

    _balances[event.customer_id] += event.points
    log(
        settings.service_name,
        "consumed",
        topic=settings.points_topic,
        message_id=message_id,
        partition=message.partition(),
        offset=message.offset(),
        customer_id=event.customer_id,
        order_id=event.order_id,
        points=event.points,
        reason=event.reason,
        balance=_balances[event.customer_id],
    )


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    settings = get_settings()
    start_health_server(settings.health_port)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "client.id": f"{settings.service_name}-0",
            "auto.offset.reset": settings.auto_offset_reset,
            "enable.auto.commit": False,
        }
    )

    log(
        settings.service_name,
        "started",
        topic=settings.points_topic,
        group_id=settings.consumer_group,
        bootstrap_servers=settings.kafka_bootstrap,
    )

    consumed = 0
    try:
        wait_for_broker(consumer, settings)
        consumer.subscribe([settings.points_topic])
        log(
            settings.service_name,
            "subscribed",
            topic=settings.points_topic,
            group_id=settings.consumer_group,
        )
        while not _shutdown.is_set():
            message = consumer.poll(settings.poll_timeout_seconds)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                log(
                    settings.service_name,
                    "consume_error",
                    topic=settings.points_topic,
                    error=str(error),
                )
                continue
            handle(message, settings)
            consumer.commit(message=message, asynchronous=False)
            consumed += 1
    finally:
        log(
            settings.service_name,
            "stopping",
            topic=settings.points_topic,
            consumed=consumed,
        )
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
