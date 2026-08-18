"""Billing tasks.

Executed by the invoice worker; enqueued by anything that imports this module.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Final

from celery import Task

from billing.celery_app import app
from billing.telemetry import log

WORKER_NAME: Final[str] = os.environ.get("SERVICE_NAME", "invoice-worker")

VAT_RATE_BASIS_POINTS: Final[int] = 2000


@app.task(bind=True, name="billing.tasks.generate_invoice", max_retries=3)
def generate_invoice(self: Task, order_id: str, amount_cents: int) -> dict[str, Any]:
    """Render the invoice document for a settled order."""
    delivery_info = self.request.delivery_info or {}
    log(
        WORKER_NAME,
        "consumed",
        topic=delivery_info.get("routing_key"),
        message_id=self.request.id,
        task=self.name,
        order_id=order_id,
        amount_cents=amount_cents,
        retries=self.request.retries,
    )

    tax_cents = round(amount_cents * VAT_RATE_BASIS_POINTS / 10000)
    invoice = {
        "invoice_id": f"INV-{order_id.removeprefix('ORD-')}",
        "order_id": order_id,
        "net_cents": amount_cents,
        "tax_cents": tax_cents,
        "gross_cents": amount_cents + tax_cents,
        "issued_at": datetime.now(timezone.utc).isoformat(),
    }

    log(
        WORKER_NAME,
        "invoice_generated",
        topic=delivery_info.get("routing_key"),
        message_id=self.request.id,
        invoice_id=invoice["invoice_id"],
        gross_cents=invoice["gross_cents"],
    )
    return invoice
