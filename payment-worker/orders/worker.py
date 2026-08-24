"""Worker entrypoint module.

``celery -A orders.worker worker`` loads this module, which pulls in the task
registry and wires the worker lifecycle into the platform's log and health
conventions.
"""

from __future__ import annotations

import os
from typing import Final

from celery.signals import worker_ready, worker_shutdown

from .celery_app import app, resolve_queue
from .observability import log, start_health_server
from .tasks import process_payment

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "payment-worker")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))

__all__ = ["app", "process_payment"]


@worker_ready.connect
def _on_worker_ready(**_: object) -> None:
    start_health_server(HEALTH_PORT)
    log(
        SERVICE_NAME,
        "started",
        topic=resolve_queue(process_payment.name),
        task=process_payment.name,
        health_port=HEALTH_PORT,
    )


@worker_shutdown.connect
def _on_worker_shutdown(**_: object) -> None:
    log(SERVICE_NAME, "stopping", topic=resolve_queue(process_payment.name))
