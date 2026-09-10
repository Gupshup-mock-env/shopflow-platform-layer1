"""Structured logging, health probe and worker lifecycle instrumentation."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import dramatiq
import pika
from pika.exceptions import AMQPError

from .config import rabbitmq_url

BROKER_CONNECT_TIMEOUT_SECONDS = 60.0


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
    service: str,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
    stop: threading.Event | None = None,
) -> None:
    """Block until RabbitMQ accepts an AMQP connection, or give up."""
    parameters = pika.URLParameters(rabbitmq_url())
    parameters.socket_timeout = 5
    parameters.blocked_connection_timeout = 5
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    while stop is None or not stop.is_set():
        attempt += 1
        try:
            pika.BlockingConnection(parameters).close()
        except (AMQPError, OSError) as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"rabbitmq unreachable after {timeout:.0f}s"
                ) from exc
            log(
                service,
                "broker_unavailable",
                attempt=attempt,
                retry_in_seconds=backoff,
                error=str(exc),
            )
            if stop is not None:
                stop.wait(backoff)
            else:
                time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)
        else:
            log(service, "broker_ready", attempts=attempt)
            return


class ObservabilityMiddleware(dramatiq.Middleware):
    """Wires worker lifecycle events into the platform log/health contract."""

    def __init__(self, service: str, port: int = 8080) -> None:
        self.service = service
        self.port = port

    def before_worker_boot(self, broker: dramatiq.Broker, worker: object) -> None:
        wait_for_broker(self.service)

    def after_worker_boot(self, broker: dramatiq.Broker, worker: object) -> None:
        start_health_server(self.port)
        log(self.service, "started", health_port=self.port)

    def before_worker_shutdown(self, broker: dramatiq.Broker, worker: object) -> None:
        log(self.service, "stopping")
