"""Request/response models for the order REST API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class OrderLine(BaseModel):
    """A single line item within an order."""

    product_id: str = Field(..., description="Stable catalogue identifier, e.g. SKU-10431")
    quantity: int = Field(..., gt=0, description="Units ordered for this line")


class OrderCreate(BaseModel):
    """Payload for placing an order."""

    customer_id: str = Field(..., min_length=1, description="Stable customer identifier")
    lines: list[OrderLine] = Field(..., min_length=1, description="At least one line item")


class Order(BaseModel):
    """A placed order, priced from the catalogue."""

    order_id: str
    customer_id: str
    lines: list[OrderLine]
    total_cents: int = Field(..., ge=0, description="Order total in minor units (USD cents)")
    status: str = Field("placed", description="Order lifecycle status")
