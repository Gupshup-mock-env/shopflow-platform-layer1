"""Alerting actors.

Imported both by the service that raises alerts and by the worker process that
delivers them, so that the two agree on actor names and signatures.
"""

from __future__ import annotations

from typing import Final

import dramatiq
from dramatiq.middleware import CurrentMessage

from .broker import SERVICE_NAME, broker  # noqa: F401  (sets the global broker)
from .observability import log

DEFAULT_CHANNEL: Final[str] = "ops-email"

CHANNELS: Final[dict[str, str]] = {
    "stockout": "merchandising-pagerduty",
    "low_stock": "merchandising-slack",
    "price_drop": "pricing-slack",
    "backorder": "supply-chain-email",
}


@dramatiq.actor(
    queue_name="alerts",
    max_retries=3,
    min_backoff=1_000,
    max_backoff=30_000,
    time_limit=30_000,
)
def send_alert(alert_type: str, sku: str) -> None:
    """Deliver a merchandising alert to the channel that owns it."""
    message = CurrentMessage.get_current_message()
    log(
        SERVICE_NAME,
        "consumed",
        topic=send_alert.queue_name,
        message_id=message.message_id if message is not None else None,
        actor=send_alert.actor_name,
        alert_type=alert_type,
        sku=sku,
    )

    channel = CHANNELS.get(alert_type, DEFAULT_CHANNEL)
    log(
        SERVICE_NAME,
        "alert_dispatched",
        alert_type=alert_type,
        sku=sku,
        channel=channel,
    )
