"""Request/response models for the inventory REST API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class StockLevel(BaseModel):
    """On-hand stock for a single product."""

    product_id: str = Field(..., description="Stable catalogue identifier, e.g. SKU-10431")
    on_hand: int = Field(..., ge=0, description="Units currently available")


class ReserveRequest(BaseModel):
    """Payload to reserve units against a product."""

    quantity: int = Field(..., gt=0, description="Units to reserve")


class ReserveResult(BaseModel):
    """The outcome of a reservation."""

    product_id: str
    reserved: int
    on_hand: int = Field(..., ge=0, description="Units remaining after the reservation")
