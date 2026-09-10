"""Notification payloads emitted by the notification service."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass
class OrderConfirmedNotification:
    """A customer-facing confirmation for an order that has been paid."""

    notification_id: str
    recipient_email: str
    order_id: str
    order_total: str
    items_summary: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: bytes | str) -> "OrderConfirmedNotification":
        payload = json.loads(raw)
        return cls(
            notification_id=payload["notification_id"],
            recipient_email=payload["recipient_email"],
            order_id=payload["order_id"],
            order_total=payload["order_total"],
            items_summary=payload["items_summary"],
        )
