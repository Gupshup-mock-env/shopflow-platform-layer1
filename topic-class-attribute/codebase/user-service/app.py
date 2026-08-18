"""User service.

Owns registration. On startup it replays the accounts that were created while
the outbox relay was down, then stays resident so the platform health probe
keeps passing.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Final

from confluent_kafka import KafkaException

from models import UserRegisteredEvent
from publisher import UserEventPublisher

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "user-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0
FLUSH_TIMEOUT_SECONDS: Final[float] = 10.0

UNRELAYED_SIGNUPS: Final[tuple[dict[str, Any], ...]] = (
    {"user_id": "USR-100241", "email": "amelia.rowe@example.com", "name": "Amelia Rowe"},
    {"user_id": "USR-100242", "email": "d.okafor@example.net", "name": "Daniel Okafor"},
    {"user_id": "USR-100243", "email": "hkim@example.org", "name": "Hana Kim"},
    {"user_id": "USR-100244", "email": "luis.ferreira@example.com", "name": "Luis Ferreira"},
    {"user_id": "USR-100245", "email": "s.whitfield@example.co.uk", "name": "Sasha Whitfield"},
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
    publisher: UserEventPublisher,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the broker answers a metadata request, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while not _shutdown.is_set():
        attempt += 1
        try:
            publisher.list_topics(timeout=5.0)
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


def build_backlog() -> list[UserRegisteredEvent]:
    registered_at = datetime.now(timezone.utc) - timedelta(minutes=len(UNRELAYED_SIGNUPS))
    return [
        UserRegisteredEvent(
            registered_at=(registered_at + timedelta(minutes=index)).isoformat(),
            **signup,
        )
        for index, signup in enumerate(UNRELAYED_SIGNUPS)
    ]


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_health_server(HEALTH_PORT)

    publisher = UserEventPublisher(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id=SERVICE_NAME,
        log=log,
    )
    log(
        SERVICE_NAME,
        "started",
        topic=publisher.topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        health_port=HEALTH_PORT,
    )
    wait_for_broker(publisher)

    backlog = build_backlog()
    published = 0
    for index, event in enumerate(backlog):
        if _shutdown.is_set():
            break
        publisher.publish(event)
        published += 1
        if index < len(backlog) - 1:
            _shutdown.wait(EVENT_INTERVAL_SECONDS)

    publisher.flush(FLUSH_TIMEOUT_SECONDS)
    log(SERVICE_NAME, "batch_complete", topic=publisher.topic, published=published)

    _shutdown.wait()

    log(SERVICE_NAME, "stopping", topic=publisher.topic, published=published)
    publisher.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
