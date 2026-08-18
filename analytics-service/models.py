"""Event payloads emitted by the analytics collector."""

from __future__ import annotations

from pydantic import BaseModel


class AnalyticsEvent(BaseModel):
    """A single client-side interaction forwarded to the warehouse."""

    event_type: str
    user_id: str
    session_id: str
    timestamp: str
    properties: dict
