"""Event models published by the loyalty platform."""

from __future__ import annotations

from pydantic import BaseModel, Field


class PointsEarnedEvent(BaseModel):
    """Loyalty points awarded to a customer for a completed order."""

    customer_id: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    points: int = Field(ge=0)
    reason: str = Field(min_length=1)
