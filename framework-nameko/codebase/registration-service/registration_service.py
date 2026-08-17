"""Registration service.

Owns ShopFlow customer sign-up. Activated accounts are drained from the
registration outbox and announced on the service event bus so that downstream
services can react without being called synchronously.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final

from nameko.events import EventDispatcher
from nameko.extensions import DependencyProvider
from nameko.timer import timer

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "registration-service")
RABBITMQ_HOST: Final[str] = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: Final[int] = int(os.environ.get("RABBITMQ_PORT", "5672"))
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

OUTBOX_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

EVENT_TOPIC: Final[str] = "registration-service.user_registered"


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


def wait_for_broker(
    host: str,
    port: int,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the broker accepts a TCP connection, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=2.0):
                log(SERVICE_NAME, "broker_ready", host=host, port=port, attempts=attempt)
                return
        except OSError as exc:
            last_error = exc
            log(
                SERVICE_NAME,
                "broker_unavailable",
                host=host,
                port=port,
                attempt=attempt,
                retry_in_seconds=backoff,
                error=str(exc),
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)
    raise TimeoutError(f"broker at {host}:{port} unreachable after {timeout:.0f}s: {last_error}")


def bootstrap() -> None:
    """Prepare the process before the Nameko runner starts the container."""
    start_health_server(HEALTH_PORT)
    wait_for_broker(RABBITMQ_HOST, RABBITMQ_PORT)


bootstrap()


PENDING_ACTIVATIONS: Final[deque[dict[str, str]]] = deque(
    [
        {"user_id": "USR-90114", "email": "amara.okafor@example.com"},
        {"user_id": "USR-90115", "email": "tomas.lindqvist@example.com"},
        {"user_id": "USR-90116", "email": "priya.raghavan@example.com"},
        {"user_id": "USR-90117", "email": "daniel.mercier@example.com"},
        {"user_id": "USR-90118", "email": "sofia.almeida@example.com"},
    ]
)

_outbox_lock = threading.Lock()


class LifecycleLogger(DependencyProvider):
    """Emits container lifecycle records onto the structured log stream."""

    def setup(self) -> None:
        log(
            SERVICE_NAME,
            "started",
            topic=EVENT_TOPIC,
            amqp_host=RABBITMQ_HOST,
            health_port=HEALTH_PORT,
        )

    def stop(self) -> None:
        log(SERVICE_NAME, "stopping", topic=EVENT_TOPIC)

    def get_dependency(self, worker_ctx: object) -> None:
        return None


class RegistrationService:
    """Account activation for the ShopFlow storefront."""

    name = "registration-service"

    dispatch = EventDispatcher()
    lifecycle = LifecycleLogger()

    @timer(interval=OUTBOX_INTERVAL_SECONDS)
    def drain_registration_outbox(self) -> None:
        with _outbox_lock:
            if not PENDING_ACTIVATIONS:
                return
            activation = PENDING_ACTIVATIONS.popleft()
        self.announce_registration(activation["user_id"], activation["email"])

    def announce_registration(self, user_id: str, email: str) -> dict[str, str]:
        payload = {"user_id": user_id, "email": email}
        self.dispatch("user_registered", payload)
        log(
            SERVICE_NAME,
            "published",
            topic=EVENT_TOPIC,
            message_id=f"msg-{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            email=email,
        )
        return payload
