"""ShopFlow storefront metrics.

Aggregates raw clickstream interactions into the counters the merchandising
dashboards read: sessions per page, add-to-cart rate and checkout starts.
"""

import asyncio
import json
import os
import socket
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Final

import faust
from faust.web import Request, Response, View

from models import ClickEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "metrics-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

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

action_totals: Counter[str] = Counter()
page_totals: Counter[str] = Counter()


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


@app.page("/metrics/actions")
class ActionTotalsView(View):
    """Running action counts, read by the merchandising dashboards."""

    async def get(self, request: Request) -> Response:
        return self.json(dict(action_totals))


def _message_id(headers: object) -> str | None:
    if not headers:
        return None
    pairs = headers.items() if isinstance(headers, dict) else headers
    for key, value in pairs:
        if key == "message-id":
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None


@app.agent(clickstream_topic)
async def track_clicks(clicks: faust.Stream[ClickEvent]) -> None:
    async for record in clicks.events():
        click: ClickEvent = record.value
        action_totals[click.action] += 1
        page_totals[click.page] += 1
        log(
            SERVICE_NAME,
            "consumed",
            topic=clickstream_topic.get_topic_name(),
            message_id=_message_id(record.message.headers),
            user_id=click.user_id,
            page=click.page,
            action=click.action,
            event_timestamp=click.timestamp,
            partition=record.message.partition,
            offset=record.message.offset,
            action_total=action_totals[click.action],
        )


@app.task
async def announce_start() -> None:
    log(
        SERVICE_NAME,
        "started",
        topic=clickstream_topic.get_topic_name(),
        broker=KAFKA_BOOTSTRAP,
        consumer_group=app.conf.id,
        web_port=HEALTH_PORT,
    )
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        log(
            SERVICE_NAME,
            "stopping",
            topic=clickstream_topic.get_topic_name(),
            consumed=sum(action_totals.values()),
        )
        raise
