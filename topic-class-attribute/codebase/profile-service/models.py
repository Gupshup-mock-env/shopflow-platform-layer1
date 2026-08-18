"""Event payloads consumed from the identity domain."""

from __future__ import annotations

from pydantic import BaseModel, Field


class UserRegisteredEvent(BaseModel):
    """Emitted once, when an account completes registration."""

    user_id: str = Field(..., description="Immutable account identifier")
    email: str = Field(..., description="Verified primary email address")
    name: str = Field(..., description="Display name supplied at signup")
    registered_at: str = Field(..., description="RFC 3339 timestamp of account creation")
