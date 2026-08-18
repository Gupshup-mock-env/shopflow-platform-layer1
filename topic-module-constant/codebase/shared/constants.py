"""Kafka topic names owned by the cart domain.

Both the producing and the consuming service import from here so that a rename
is a single-line change. Topic names follow the platform convention
``shopflow.<domain>.<event>``.
"""

from __future__ import annotations

CART_EVENTS_TOPIC = "shopflow.cart.updated"

DEFAULT_CONSUMER_GROUP = "pricing-service"

DEFAULT_PARTITION_COUNT = 3
