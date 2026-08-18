"""ShopFlow pricing service.

Owns the effective sell price of every catalogue product. Two things change a
price: the nightly repricing run, which walks the catalogue and applies planned
markdowns, and merchandiser clearance overrides, which are applied the moment
they are entered in the back office.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Final
from uuid import uuid4

from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.asgi.types import Scope
from faststream.kafka import KafkaBroker

from models import PriceUpdatedEvent

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "pricing-service")
KAFKA_BOOTSTRAP: Final[str] = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

EVENT_INTERVAL_SECONDS: Final[float] = 2.0
BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0

# (product_id, old price in cents, new price in cents)
PriceChange = tuple[str, int, int]

SCHEDULED_MARKDOWNS: Final[tuple[PriceChange, ...]] = (
    ("SKU-ESPRESSO-DUO", 18900, 15900),
    ("SKU-MILK-FROTHER", 4500, 3800),
    ("SKU-BEANS-COL-1KG", 2400, 2150),
)

CLEARANCE_OVERRIDES: Final[tuple[PriceChange, ...]] = (
    ("SKU-TRAVEL-MUG-BLU", 2600, 999),
    ("SKU-FILTER-CONE-V2", 1800, 750),
)

broker = KafkaBroker(
    KAFKA_BOOTSTRAP,
    client_id=SERVICE_NAME,
    acks="all",
    logger=None,
)

price_events = broker.publisher(
    "shopflow.pricing.updated",
    description="Effective price changes for products in the ShopFlow catalogue.",
)

_background_tasks: set[asyncio.Task[None]] = set()


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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@price_events
async def scheduled_markdown(
    product_id: str,
    old_price_cents: int,
    new_price_cents: int,
) -> PriceUpdatedEvent:
    """Build the price change produced by the nightly repricing run."""
    return PriceUpdatedEvent(
        product_id=product_id,
        old_price_cents=old_price_cents,
        new_price_cents=new_price_cents,
        effective_at=_now(),
    )


async def clearance_override(
    product_id: str,
    old_price_cents: int,
    new_price_cents: int,
    correlation_id: str,
) -> PriceUpdatedEvent:
    """Apply a manual clearance price straight away, outside the nightly run."""
    event = PriceUpdatedEvent(
        product_id=product_id,
        old_price_cents=old_price_cents,
        new_price_cents=new_price_cents,
        effective_at=_now(),
    )
    await broker.publish(
        event,
        "shopflow.pricing.updated",
        key=product_id.encode("utf-8"),
        correlation_id=correlation_id,
        headers={"x-price-change-reason": "clearance"},
    )
    return event


async def _run_price_changes() -> None:
    published = 0

    for product_id, old_price_cents, new_price_cents in SCHEDULED_MARKDOWNS:
        correlation_id = str(uuid4())
        event = await scheduled_markdown(product_id, old_price_cents, new_price_cents)
        await price_events.publish(
            event,
            key=product_id.encode("utf-8"),
            correlation_id=correlation_id,
            headers={"x-price-change-reason": "scheduled-markdown"},
        )
        published += 1
        log(
            SERVICE_NAME,
            "published",
            topic="shopflow.pricing.updated",
            message_id=correlation_id,
            product_id=event.product_id,
            old_price_cents=event.old_price_cents,
            new_price_cents=event.new_price_cents,
            reason="scheduled-markdown",
        )
        await asyncio.sleep(EVENT_INTERVAL_SECONDS)

    for product_id, old_price_cents, new_price_cents in CLEARANCE_OVERRIDES:
        correlation_id = str(uuid4())
        event = await clearance_override(
            product_id,
            old_price_cents,
            new_price_cents,
            correlation_id,
        )
        published += 1
        log(
            SERVICE_NAME,
            "published",
            topic="shopflow.pricing.updated",
            message_id=correlation_id,
            product_id=event.product_id,
            old_price_cents=event.old_price_cents,
            new_price_cents=event.new_price_cents,
            reason="clearance",
        )
        await asyncio.sleep(EVENT_INTERVAL_SECONDS)

    await price_events.flush()
    log(
        SERVICE_NAME,
        "batch_complete",
        topic="shopflow.pricing.updated",
        published=published,
    )


@app.on_startup
async def connect_to_broker() -> None:
    await wait_for_broker()


@app.after_startup
async def start_price_changes() -> None:
    log(
        SERVICE_NAME,
        "started",
        topic="shopflow.pricing.updated",
        bootstrap_servers=KAFKA_BOOTSTRAP,
    )
    task = asyncio.create_task(_run_price_changes())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


@app.on_shutdown
async def stop_price_changes() -> None:
    for task in list(_background_tasks):
        task.cancel()
    log(SERVICE_NAME, "stopping", topic="shopflow.pricing.updated")
