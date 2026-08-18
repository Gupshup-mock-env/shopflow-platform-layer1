"""Order event payloads consumed by the payment service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    """A single line item on a customer order."""

    sku: str
    quantity: int = Field(gt=0)
    price_cents: int = Field(ge=0)


class OrderCreatedEvent(BaseModel):
    """Emitted once checkout has accepted and priced an order."""

    order_id: str
    customer_id: str
    total_cents: int = Field(ge=0)
    currency: str
    items: list[OrderItem]

    def line_total_cents(self) -> int:
        return sum(item.quantity * item.price_cents for item in self.items)
