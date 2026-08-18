"""Welcome service.

Sends the ShopFlow onboarding email when a customer account is activated. The
service subscribes to the registration service's event stream; it never calls
registration back.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Final

from nameko.events import event_handler
from nameko.extensions import DependencyProvider

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "welcome-service")
RABBITMQ_HOST: Final[str] = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: Final[int] = int(os.environ.get("RABBITMQ_PORT", "5672"))
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

EVENT_TOPIC: Final[str] = "registration-service.user_registered"
WELCOME_TEMPLATE_ID: Final[str] = "shopflow-welcome-v3"


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


class WelcomeService:
    """Customer onboarding messaging for the ShopFlow storefront."""

    name = "welcome-service"

    lifecycle = LifecycleLogger()

    @event_handler("registration-service", "user_registered")
    def send_welcome_email(self, payload: dict[str, str]) -> None:
        log(
            SERVICE_NAME,
            "consumed",
            topic=EVENT_TOPIC,
            message_id=f"msg-{uuid.uuid4().hex[:12]}",
            user_id=payload["user_id"],
            email=payload["email"],
            template_id=WELCOME_TEMPLATE_ID,
        )
