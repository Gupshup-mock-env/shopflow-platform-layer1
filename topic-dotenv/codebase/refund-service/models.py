"""Return payloads accepted by the refund service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ReturnInitiatedEvent(BaseModel):
    """A customer has started a return against a delivered order."""

    return_id: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    items: list[str] = Field(min_length=1)
