"""ShopFlow discount service.

Recalculates the promotional discount ladder for a product whenever its sell
price changes, so campaign percentages never drift away from the price the
storefront is actually showing.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Final

from faststream import AckPolicy
from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.asgi.types import Scope
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import KafkaMessage

from models import PriceUpdatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "discount-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP: Final[str] = os.environ.get("KAFKA_CONSUMER_GROUP", "discount-service")

DISCOUNT_TIERS: Final[tuple[int, ...]] = (5, 10, 20)
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

broker = KafkaBroker(
    KAFKA_BOOTSTRAP,
    client_id=SERVICE_NAME,
    logger=None,
)


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


async def wait_for_broker(timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS) -> None:
    """Block until a bootstrap server accepts a connection, or give up."""
    deadline = time.monotonic() + timeout
    backoff = 0.5
    attempt = 0
    last_error = "no bootstrap servers configured"

    while True:
        attempt += 1
        for host, port in _bootstrap_endpoints(KAFKA_BOOTSTRAP):
            try:
                _, writer = await asyncio.open_connection(host, port)
            except OSError as exc:
                last_error = f"{host}:{port}: {exc}"
                continue
            writer.close()
            await writer.wait_closed()
            log(
                SERVICE_NAME,
                "broker_ready",
                bootstrap_servers=KAFKA_BOOTSTRAP,
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
            bootstrap_servers=KAFKA_BOOTSTRAP,
            attempt=attempt,
            retry_in_seconds=backoff,
            error=last_error,
        )
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 5.0)


@get
async def healthz(scope: Scope) -> AsgiResponse:
    """Liveness and readiness probe."""
    return AsgiResponse(b"ok", 200, {"content-type": "text/plain"})


app = AsgiFastStream(
    broker,
    asgi_routes=[("/healthz", healthz)],
    logger=None,
)


def rebuild_ladder(price_cents: int) -> dict[str, int]:
    """Recompute the discounted price points for a product."""
    return {
        f"tier_{percent}": round(price_cents * (100 - percent) / 100)
        for percent in DISCOUNT_TIERS
    }


@broker.subscriber(
    "shopflow.pricing.updated",
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    ack_policy=AckPolicy.ACK,
    description="Keeps promotional discount ladders in step with catalogue prices.",
)
async def handle_price_update(msg: PriceUpdatedEvent, message: KafkaMessage) -> None:
    ladder = rebuild_ladder(msg.new_price_cents)
    log(
        SERVICE_NAME,
        "consumed",
        topic="shopflow.pricing.updated",
        message_id=message.correlation_id,
        product_id=msg.product_id,
        old_price_cents=msg.old_price_cents,
        new_price_cents=msg.new_price_cents,
        effective_at=msg.effective_at,
        ladder=ladder,
    )


@app.on_startup
async def connect_to_broker() -> None:
    await wait_for_broker()


@app.after_startup
async def announce_start() -> None:
    log(
        SERVICE_NAME,
        "started",
        topic="shopflow.pricing.updated",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group=CONSUMER_GROUP,
    )


@app.on_shutdown
async def announce_stop() -> None:
    log(
        SERVICE_NAME,
        "stopping",
        topic="shopflow.pricing.updated",
        consumer_group=CONSUMER_GROUP,
    )
