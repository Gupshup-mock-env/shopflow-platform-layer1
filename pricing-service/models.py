"""Event payloads owned by the pricing service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class PriceUpdatedEvent(BaseModel):
    """A confirmed price change for a single catalogue product."""

    product_id: str
    old_price_cents: int = Field(ge=0)
    new_price_cents: int = Field(ge=0)
    effective_at: str
