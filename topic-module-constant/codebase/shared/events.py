"""Event models for the cart domain."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CartUpdatedEvent(BaseModel):
    """Emitted whenever the contents or totals of a cart change."""

    cart_id: str = Field(..., description="Cart identifier, stable for the session")
    customer_id: str = Field(..., description="Owning customer, or a guest token")
    item_count: int = Field(..., ge=0, description="Number of line items in the cart")
    total_cents: int = Field(..., ge=0, description="Cart subtotal in minor units (USD cents)")
