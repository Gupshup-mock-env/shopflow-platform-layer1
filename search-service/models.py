"""Event payloads consumed from the catalogue stream."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProductUpdatedEvent(BaseModel):
    """A merchandiser-visible change to a catalogue product."""

    product_id: str = Field(..., description="Stable catalogue identifier, e.g. SKU-10431")
    name: str = Field(..., description="Storefront display name")
    price_cents: int = Field(..., ge=0, description="List price in minor units (USD cents)")
    category: str = Field(..., description="Slash-delimited category path")
