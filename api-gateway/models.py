"""Models for the API gateway.

The gateway is a thin reverse proxy: it forwards request/response bodies as-is,
so it keeps no domain models of its own beyond a small route descriptor.
"""

from __future__ import annotations

from pydantic import BaseModel


class RouteInfo(BaseModel):
    """A registered upstream route, surfaced by ``GET /routes``."""

    prefix: str
    upstream: str
