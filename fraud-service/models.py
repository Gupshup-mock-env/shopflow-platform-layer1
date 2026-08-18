"""Event payloads emitted by the fraud screening service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FraudCheckRequestedEvent(BaseModel):
    """Raised when an order needs a synchronous fraud decision."""

    order_id: str
    amount_cents: int = Field(ge=0)
    customer_id: str
    ip_address: str
