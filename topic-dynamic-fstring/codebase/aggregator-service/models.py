"""Event payloads accepted by the analytics aggregator."""

from __future__ import annotations

from pydantic import BaseModel


class AnalyticsEvent(BaseModel):
    """Inbound client-side interaction."""

    event_type: str
    user_id: str
    session_id: str
    timestamp: str
    properties: dict
