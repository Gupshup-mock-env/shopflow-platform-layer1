"""Inventory service (REST).

Tracks on-hand stock and reserves it. A leaf service: it calls no other service.
The order service reserves stock against it over HTTP. On-hand counts live in an
in-process dictionary here; the production deployment writes through to Postgres.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Final

from fastapi import FastAPI, HTTPException

from models import ReserveRequest, ReserveResult, StockLevel

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "inventory-service")
INITIAL_ON_HAND: Final[int] = int(os.environ.get("INITIAL_ON_HAND", "100"))

app = FastAPI(title=SERVICE_NAME)

_ON_HAND: dict[str, int] = defaultdict(lambda: INITIAL_ON_HAND)


def log(event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stock/{product_id}")
def get_stock(product_id: str) -> StockLevel:
    level = StockLevel(product_id=product_id, on_hand=_ON_HAND[product_id])
    log("get_stock", product_id=product_id, on_hand=level.on_hand)
    return level


@app.post("/stock/{product_id}/reserve")
def reserve(product_id: str, body: ReserveRequest) -> ReserveResult:
    available = _ON_HAND[product_id]
    if body.quantity > available:
        log(
            "reserve_rejected",
            product_id=product_id,
            requested=body.quantity,
            on_hand=available,
        )
        raise HTTPException(
            status_code=409,
            detail=f"insufficient stock for {product_id}: {available} on hand",
        )
    _ON_HAND[product_id] = available - body.quantity
    result = ReserveResult(
        product_id=product_id,
        reserved=body.quantity,
        on_hand=_ON_HAND[product_id],
    )
    log("reserved", product_id=product_id, reserved=body.quantity, on_hand=result.on_hand)
    return result
