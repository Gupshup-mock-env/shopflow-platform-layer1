"""API gateway (REST).

The storefront's single front door. Every public request is a REST call that the
gateway forwards to exactly one upstream service over HTTP:

  * ``/api/catalog/*``   -> catalog-service
  * ``/api/search/*``    -> search-service
  * ``/api/orders/*``    -> order-service
  * ``/api/inventory/*`` -> inventory-service

The gateway holds no state and speaks only HTTP to its upstreams.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Final

import httpx
from fastapi import FastAPI, Request, Response

from models import RouteInfo

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "api-gateway")
HTTP_TIMEOUT_SECONDS: Final[float] = 5.0

UPSTREAMS: Final[dict[str, str]] = {
    "catalog": os.environ.get("CATALOG_URL", "http://catalog-service:8080"),
    "search": os.environ.get("SEARCH_URL", "http://search-service:8080"),
    "orders": os.environ.get("ORDER_URL", "http://order-service:8080"),
    "inventory": os.environ.get("INVENTORY_URL", "http://inventory-service:8080"),
}

app = FastAPI(title=SERVICE_NAME)


def log(event: str, **fields: object) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "event": event,
        **fields,
    }
    print(json.dumps(record), flush=True)


async def forward(method: str, url: str, body: bytes, params: bytes) -> httpx.Response:
    """Forward a request to an upstream service and return its raw response."""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        return await client.request(
            method,
            url,
            content=body or None,
            params=httpx.QueryParams(params.decode("utf-8")) if params else None,
            headers={"content-type": "application/json"},
        )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/routes")
def routes() -> list[RouteInfo]:
    return [RouteInfo(prefix=f"/api/{name}", upstream=url) for name, url in UPSTREAMS.items()]


@app.api_route(
    "/api/{service}/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE"],
)
async def proxy(service: str, path: str, request: Request) -> Response:
    upstream = UPSTREAMS.get(service)
    if upstream is None:
        return Response(
            content=json.dumps({"detail": f"unknown service {service}"}),
            status_code=404,
            media_type="application/json",
        )

    url = f"{upstream}/{path}"
    body = await request.body()
    upstream_resp = await forward(
        request.method, url, body, request.url.query.encode("utf-8")
    )
    log("proxied", service=service, path=path, status=upstream_resp.status_code)
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        media_type=upstream_resp.headers.get("content-type", "application/json"),
    )
