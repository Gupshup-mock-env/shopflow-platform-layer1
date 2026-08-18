"""Domain events exchanged between the ShopFlow inventory services."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StockUpdate:
    """A single movement of stock in or out of a warehouse."""

    sku: str
    warehouse_id: str
    quantity_delta: int
    reason: str
