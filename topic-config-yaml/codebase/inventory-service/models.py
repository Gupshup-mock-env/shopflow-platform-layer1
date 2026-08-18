"""Stock movement payloads published by the inventory ledger."""

from __future__ import annotations

from pydantic import BaseModel, Field


class StockUpdatedEvent(BaseModel):
    """A single movement applied to the on-hand quantity of a SKU."""

    sku: str = Field(min_length=1)
    warehouse_id: str = Field(min_length=1)
    quantity_delta: int
    reason: str = Field(min_length=1)
