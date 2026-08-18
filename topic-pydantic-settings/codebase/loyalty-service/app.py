"""Loyalty service: awards points for completed orders and publishes them to Kafka."""

from __future__ import annotations

import json
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from confluent_kafka import KafkaException, Message, Producer

from events import PointsEarnedEvent
from settings import Settings, get_settings

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


SAMPLE_AWARDS: tuple[PointsEarnedEvent, ...] = (
    PointsEarnedEvent(
        customer_id="CUST-4471",
        order_id="ORD-889201",
        points=120,
        reason="order_completed",
    ),
    PointsEarnedEvent(
        customer_id="CUST-1180",
        order_id="ORD-889244",
        points=45,
        reason="order_completed",
    ),
    PointsEarnedEvent(
        customer_id="CUST-9032",
        order_id="ORD-889310",
        points=500,
        reason="tier_upgrade_bonus",
    ),
    PointsEarnedEvent(
        customer_id="CUST-4471",
        order_id="ORD-889377",
        points=75,
        reason="referral_credit",
    ),
    PointsEarnedEvent(
        customer_id="CUST-6624",
        order_id="ORD-889402",
        points=260,
        reason="order_completed",
    ),
)


def wait_for_broker(producer: Producer, settings: Settings, timeout_seconds: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    delay = 0.5
    last_error = "unknown"
    while time.monotonic() < deadline and not _shutdown.is_set():
        try:
            producer.list_topics(timeout=5.0)
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


def publish_samples(producer: Producer, settings: Settings) -> int:
    published = 0
    for index, event in enumerate(SAMPLE_AWARDS):
        if _shutdown.is_set():
            break
        message_id = f"pts-{uuid.uuid4().hex[:12]}"

        def _on_delivery(error: object, message: Message, message_id: str = message_id) -> None:
            if error is not None:
                log(
                    settings.service_name,
                    "delivery_failed",
                    topic=message.topic(),
                    message_id=message_id,
                    error=str(error),
                )

        producer.produce(
            settings.points_topic,
            key=event.customer_id.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
            headers={"message_id": message_id, "content-type": "application/json"},
            on_delivery=_on_delivery,
        )
        producer.poll(0)
        log(
            settings.service_name,
            "published",
            topic=settings.points_topic,
            message_id=message_id,
            customer_id=event.customer_id,
            order_id=event.order_id,
            points=event.points,
        )
        published += 1
        if index < len(SAMPLE_AWARDS) - 1:
            _shutdown.wait(settings.publish_interval_seconds)
    producer.flush(settings.delivery_timeout_seconds)
    return published


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    settings = get_settings()
    start_health_server(settings.health_port)

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": f"{settings.service_name}-0",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
        }
    )

    log(
        settings.service_name,
        "started",
        topic=settings.points_topic,
        bootstrap_servers=settings.kafka_bootstrap,
    )

    wait_for_broker(producer, settings)
    published = publish_samples(producer, settings)
    log(
        settings.service_name,
        "batch_complete",
        topic=settings.points_topic,
        published=published,
    )

    _shutdown.wait()
    producer.flush(settings.delivery_timeout_seconds)
    log(
        settings.service_name,
        "stopping",
        topic=settings.points_topic,
        published=published,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
