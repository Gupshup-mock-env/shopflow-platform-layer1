"""Event payloads accepted by the manual review service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FraudCheckRequestedEvent(BaseModel):
    """Inbound fraud screening request."""

    order_id: str
    amount_cents: int = Field(ge=0)
    customer_id: str
    ip_address: str
