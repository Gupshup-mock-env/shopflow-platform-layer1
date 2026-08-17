"""Broker readiness helpers shared by billing components."""

from __future__ import annotations

from typing import Final

from billing.celery_app import app
from billing.telemetry import log

BROKER_CONNECT_TIMEOUT_SECONDS: Final[float] = 60.0


def wait_for_broker(
    service: str,
    timeout: float = BROKER_CONNECT_TIMEOUT_SECONDS,
) -> None:
    """Block until the AMQP broker accepts a connection, or give up."""

    def on_error(exc: BaseException, interval: float) -> None:
        log(
            service,
            "broker_unavailable",
            broker_url=app.conf.broker_url,
            retry_in_seconds=round(interval, 2),
            error=str(exc),
        )

    with app.connection() as connection:
        connection.ensure_connection(
            errback=on_error,
            max_retries=None,
            interval_start=0.5,
            interval_step=0.5,
            interval_max=5.0,
            timeout=timeout,
        )
    log(service, "broker_ready", broker_url=app.conf.broker_url)
