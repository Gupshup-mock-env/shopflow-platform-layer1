"""Order service (REST).

Places orders by orchestrating two downstream services over HTTP:

  * ``catalog-service`` — priced line items are read from ``GET /products/{id}``
  * ``inventory-service`` — stock is reserved via ``POST /stock/{id}/reserve``

No message broker is involved; every cross-service interaction is a REST call.
Orders live in an in-process dictionary here; production writes through to Postgres.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Final
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException

from models import Order, OrderCreate

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "order-service")
CATALOG_URL: Final[str] = os.environ.get("CATALOG_URL", "http://catalog-service:8080")
INVENTORY_URL: Final[str] = os.environ.get("INVENTORY_URL", "http://inventory-service:8080")
HTTP_TIMEOUT_SECONDS: Final[float] = 5.0

app = FastAPI(title=SERVICE_NAME)

_ORDERS: dict[str, Order] = {}


def log(event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


async def fetch_product(client: httpx.AsyncClient, product_id: str) -> dict[str, Any]:
    """Read a single product from catalog-service."""
    resp = await client.get(f"{CATALOG_URL}/products/{product_id}")
    if resp.status_code == 404:
        raise HTTPException(status_code=400, detail=f"unknown product {product_id}")
    resp.raise_for_status()
    return resp.json()


async def reserve_stock(
    client: httpx.AsyncClient, product_id: str, quantity: int
) -> dict[str, Any]:
    """Reserve stock for a product via inventory-service."""
    resp = await client.post(
        f"{INVENTORY_URL}/stock/{product_id}/reserve",
        json={"quantity": quantity},
    )
    if resp.status_code == 409:
        raise HTTPException(status_code=409, detail=f"insufficient stock for {product_id}")
    resp.raise_for_status()
    return resp.json()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/orders", status_code=201)
async def place_order(body: OrderCreate) -> Order:
    order_id = f"ORD-{uuid4().hex[:8]}"
    total = 0
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        for line in body.lines:
            product = await fetch_product(client, line.product_id)
            await reserve_stock(client, line.product_id, line.quantity)
            total += int(product["price_cents"]) * line.quantity

    order = Order(
        order_id=order_id,
        customer_id=body.customer_id,
        lines=body.lines,
        total_cents=total,
        status="placed",
    )
    _ORDERS[order_id] = order
    log("order_placed", order_id=order_id, total_cents=total, lines=len(body.lines))
    return order


@app.get("/orders/{order_id}")
def get_order(order_id: str) -> Order:
    order = _ORDERS.get(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"unknown order {order_id}")
    return order
