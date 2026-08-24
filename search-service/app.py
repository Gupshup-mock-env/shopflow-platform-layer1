"""Search service (REST).

Answers storefront search queries by reading the catalogue from
``catalog-service`` over HTTP and filtering in-process. No broker, no local
datastore — every query fans out to a REST call against the catalogue.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Final

import httpx
from fastapi import FastAPI

from models import SearchHit, SearchResponse

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "search-service")
CATALOG_URL: Final[str] = os.environ.get("CATALOG_URL", "http://catalog-service:8080")
HTTP_TIMEOUT_SECONDS: Final[float] = 5.0

app = FastAPI(title=SERVICE_NAME)


def log(event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


async def fetch_catalog(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Read the full catalogue from catalog-service."""
    resp = await client.get(f"{CATALOG_URL}/products")
    resp.raise_for_status()
    return resp.json()


def match(product: dict[str, Any], query: str) -> bool:
    needle = query.casefold()
    return needle in product["name"].casefold() or needle in product["category"].casefold()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/search")
async def search(q: str) -> SearchResponse:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        products = await fetch_catalog(client)

    hits = [
        SearchHit(
            product_id=p["product_id"],
            name=p["name"],
            category=p["category"],
            price_cents=p["price_cents"],
        )
        for p in products
        if match(p, q)
    ]
    log("search", query=q, hits=len(hits))
    return SearchResponse(query=q, hits=hits)
