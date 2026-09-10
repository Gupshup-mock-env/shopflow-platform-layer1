"""ShopFlow clickstream collector.

Receives storefront interactions from the web tier and writes them onto the raw
clickstream so the analytics estate has a single ordered record of what
shoppers did.
"""

import asyncio
import json
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any, Final
from uuid import uuid4

import faust
from faust.web import Request, Response, View

from models import ClickEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "clickstream-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

# (user_id, page, action)
SAMPLE_SESSION: Final[tuple[tuple[str, str, str], ...]] = (
    ("USR-40183", "/c/espresso-machines", "page_view"),
    ("USR-40183", "/p/SKU-ESPRESSO-DUO", "page_view"),
    ("USR-77420", "/p/SKU-MILK-FROTHER", "add_to_cart"),
    ("USR-40183", "/cart", "page_view"),
    ("USR-51996", "/checkout", "begin_checkout"),
)

LOGGING_CONFIG: Final[dict[str, Any]] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "plain": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"},
    },
    "handlers": {
        "stderr": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "formatter": "plain",
        },
    },
    "root": {"level": "INFO", "handlers": ["stderr"]},
}

app = faust.App(
    SERVICE_NAME,
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    store="memory://",
    topic_partitions=3,
    topic_replication_factor=1,
    web_bind="0.0.0.0",
    web_port=HEALTH_PORT,
    worker_redirect_stdouts=False,
    logging_config=LOGGING_CONFIG,
)

clickstream_topic = app.topic("shopflow.clickstream.raw", value_type=ClickEvent)


def log(service: str, event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


def _bootstrap_endpoints(bootstrap: str) -> list[tuple[str, int]]:
    endpoints: list[tuple[str, int]] = []
    for server in bootstrap.split(","):
        server = server.strip()
        if not server:
            continue
        host, separator, port = server.rpartition(":")
        if separator:
            endpoints.append((host, int(port)))
        else:
            endpoints.append((server, 9092))
    return endpoints


def wait_for_broker(timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS) -> None:
    """Block until a bootstrap server accepts a connection, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    last_error = "no bootstrap servers configured"

    while True:
        attempt += 1
        for host, port in _bootstrap_endpoints(KAFKA_BOOTSTRAP):
            try:
                with socket.create_connection((host, port), timeout=5.0):
                    pass
            except OSError as exc:
                last_error = f"{host}:{port}: {exc}"
                continue
            log(
                SERVICE_NAME,
                "broker_ready",
                broker=KAFKA_BOOTSTRAP,
                attempts=attempt,
            )
            return

        if time.monotonic() + backoff >= deadline:
            raise RuntimeError(
                f"Kafka at {KAFKA_BOOTSTRAP} unreachable after {timeout:.0f}s: {last_error}"
            )

        log(
            SERVICE_NAME,
            "broker_unavailable",
            broker=KAFKA_BOOTSTRAP,
            attempt=attempt,
            retry_in_seconds=backoff,
            error=last_error,
        )
        time.sleep(backoff)
        backoff = min(backoff * 2, 5.0)


@app.on_worker_init.connect
def block_until_broker_ready(app_: faust.App, **kwargs: object) -> None:
    wait_for_broker()


@app.page("/healthz")
class HealthView(View):
    """Liveness and readiness probe."""

    async def get(self, request: Request) -> Response:
        return self.json({"status": "ok"})


@app.task
async def replay_sample_session() -> None:
    """Write a short burst of interactions onto the clickstream at startup."""
    log(
        SERVICE_NAME,
        "started",
        topic=clickstream_topic.get_topic_name(),
        broker=KAFKA_BOOTSTRAP,
        web_port=HEALTH_PORT,
    )

    published = 0
    for user_id, page, action in SAMPLE_SESSION:
        message_id = str(uuid4())
        event = ClickEvent(
            user_id=user_id,
            page=page,
            action=action,
            timestamp=time.time(),
        )
        await clickstream_topic.send(
            value=event,
            key=user_id,
            headers={"message-id": message_id.encode("utf-8")},
        )
        published += 1
        log(
            SERVICE_NAME,
            "published",
            topic=clickstream_topic.get_topic_name(),
            message_id=message_id,
            user_id=event.user_id,
            page=event.page,
            action=event.action,
        )
        await asyncio.sleep(EVENT_INTERVAL_SECONDS)

    log(
        SERVICE_NAME,
        "batch_complete",
        topic=clickstream_topic.get_topic_name(),
        published=published,
    )


@app.task
async def trace_lifecycle() -> None:
    """Record the shutdown of the worker once the platform sends SIGTERM."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        log(SERVICE_NAME, "stopping", topic=clickstream_topic.get_topic_name())
        raise
