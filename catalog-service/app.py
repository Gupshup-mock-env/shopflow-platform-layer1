"""Catalogue service (REST).

Owns the product catalogue and serves it over HTTP. This is a leaf service: it
calls no other service. Downstream services (search, order) read from it via its
REST API. Products live in an in-process dictionary here; the production
deployment writes through to Postgres.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Final

from fastapi import FastAPI, HTTPException

from models import Product, ProductCreate

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "catalog-service")

app = FastAPI(title=SERVICE_NAME)

_PRODUCTS: dict[str, Product] = {
    "SKU-10431": Product(
        product_id="SKU-10431",
        name="Aeropress Go Travel Press",
        price_cents=3999,
        category="kitchen/coffee",
    ),
    "SKU-20887": Product(
        product_id="SKU-20887",
        name="Merino Wool Crew Socks, 3 pack",
        price_cents=2450,
        category="apparel/socks",
    ),
    "SKU-33125": Product(
        product_id="SKU-33125",
        name="Cast Iron Skillet 12 inch",
        price_cents=5495,
        category="kitchen/cookware",
    ),
}


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


@app.get("/products")
def list_products(category: str | None = None) -> list[Product]:
    products = list(_PRODUCTS.values())
    if category is not None:
        products = [p for p in products if p.category.startswith(category)]
    log("list_products", count=len(products), category=category)
    return products


@app.get("/products/{product_id}")
def get_product(product_id: str) -> Product:
    product = _PRODUCTS.get(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail=f"unknown product {product_id}")
    log("get_product", product_id=product_id)
    return product


@app.post("/products/{product_id}", status_code=201)
def upsert_product(product_id: str, body: ProductCreate) -> Product:
    product = Product(product_id=product_id, **body.model_dump())
    _PRODUCTS[product_id] = product
    log("upsert_product", product_id=product_id, category=product.category)
    return product
