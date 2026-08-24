"""Request/response models for the catalogue REST API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProductCreate(BaseModel):
    """Payload for creating or replacing a product."""

    name: str = Field(..., min_length=1, description="Storefront display name")
    price_cents: int = Field(..., ge=0, description="List price in minor units (USD cents)")
    category: str = Field(..., min_length=1, description="Slash-delimited category path")


class Product(ProductCreate):
    """A catalogue product as returned by the API."""

    product_id: str = Field(..., description="Stable catalogue identifier, e.g. SKU-10431")
