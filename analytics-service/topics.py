"""Destination naming for the ShopFlow regional analytics stream.

Every region owns its own set of per-event-type streams so a regional outage
cannot back up traffic for the rest of the fleet.
"""

from __future__ import annotations

import os
from typing import Final

DEFAULT_REGION: Final[str] = "us-east-1"

TRACKED_EVENT_TYPES: Final[tuple[str, ...]] = ("page_view", "purchase", "click")


def current_region() -> str:
    """Region this replica is pinned to."""
    return os.environ.get("REGION", DEFAULT_REGION)


def topic_for(region: str, event_type: str) -> str:
    return f"shopflow.analytics.{region}.{event_type}"
