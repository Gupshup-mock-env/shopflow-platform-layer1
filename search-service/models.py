"""Response models for the search REST API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SearchHit(BaseModel):
    """A single product matched by a query."""

    product_id: str
    name: str
    category: str
    price_cents: int = Field(..., ge=0)


class SearchResponse(BaseModel):
    """A search query and its matches."""

    query: str
    hits: list[SearchHit]
