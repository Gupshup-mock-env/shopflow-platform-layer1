"""Event payloads published by the order service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class OrderLine(BaseModel):
    """A single line item within a placed order."""

    product_id: str = Field(..., description="Stable catalogue identifier, e.g. SKU-10431")
    quantity: int = Field(..., gt=0, description="Units ordered for this line")
    unit_price_cents: int = Field(..., ge=0, description="Unit price in minor units (USD cents)")


class OrderPlacedEvent(BaseModel):
    """A checkout that has been accepted and paid for."""

    order_id: str = Field(..., description="Stable order identifier, e.g. ORD-88213")
    customer_id: str = Field(..., description="Stable customer identifier")
    currency: str = Field("USD", min_length=3, max_length=3, description="ISO-4217 currency code")
    lines: list[OrderLine] = Field(..., min_length=1, description="At least one line item")

    @property
    def total_cents(self) -> int:
        """Order total in minor units, summed across all lines."""
        return sum(line.quantity * line.unit_price_cents for line in self.lines)
