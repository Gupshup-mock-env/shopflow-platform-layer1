"""Order pipeline tasks.

This module is the contract between the service that enqueues work and the
worker that executes it: both import ``process_payment`` from here.
"""

from __future__ import annotations

import hashlib
import os
from typing import Final

from celery import Task

from .celery_app import app, resolve_queue
from .observability import log

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "payment-worker")

CAPTURE_CURRENCY: Final[str] = "USD"


def authorization_code(order_id: str, amount_cents: int) -> str:
    digest = hashlib.sha1(f"{order_id}:{amount_cents}".encode("utf-8")).hexdigest()
    return f"AUTH-{digest[:10].upper()}"


@app.task(
    bind=True,
    name="orders.tasks.process_payment",
    acks_late=True,
    max_retries=3,
    retry_backoff=True,
    retry_backoff_max=30,
    autoretry_for=(ConnectionError, TimeoutError),
)
def process_payment(self: Task, order_id: str, amount_cents: int) -> dict[str, object]:
    """Capture the authorised amount for an order."""
    queue = resolve_queue(self.name)
    log(
        SERVICE_NAME,
        "consumed",
        topic=queue,
        message_id=self.request.id,
        task=self.name,
        order_id=order_id,
        amount_cents=amount_cents,
        retries=self.request.retries,
    )

    if amount_cents <= 0:
        log(
            SERVICE_NAME,
            "payment_rejected",
            order_id=order_id,
            amount_cents=amount_cents,
            reason="non_positive_amount",
        )
        return {"order_id": order_id, "status": "rejected"}

    authorization = authorization_code(order_id, amount_cents)
    log(
        SERVICE_NAME,
        "payment_captured",
        order_id=order_id,
        amount_cents=amount_cents,
        currency=CAPTURE_CURRENCY,
        authorization=authorization,
    )
    return {
        "order_id": order_id,
        "status": "captured",
        "amount_cents": amount_cents,
        "currency": CAPTURE_CURRENCY,
        "authorization": authorization,
    }
