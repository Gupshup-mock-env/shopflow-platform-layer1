"""ShopFlow invoice worker.

Runs an in-process Celery worker that serves every queue the billing task
routes point at. Celery installs its own SIGTERM and SIGINT handlers once the
worker is running, so shutdown is warm: in-flight tasks finish and are acked
before the process exits.
"""

from __future__ import annotations

import os
from typing import Final

from billing.broker import wait_for_broker
from billing.celery_app import app, routed_queues
from billing.telemetry import log, start_health_server

SERVICE_NAME: Final[str] = os.environ.get("SERVICE_NAME", "invoice-worker")
HEALTH_PORT: Final[int] = int(os.environ.get("HEALTH_PORT", "8080"))
CONCURRENCY: Final[int] = int(os.environ.get("WORKER_CONCURRENCY", "1"))


def main() -> None:
    start_health_server(HEALTH_PORT)

    queues = routed_queues()
    log(
        SERVICE_NAME,
        "started",
        topics=queues,
        concurrency=CONCURRENCY,
        health_port=HEALTH_PORT,
    )

    wait_for_broker(SERVICE_NAME)

    app.worker_main(
        [
            "--quiet",
            "worker",
            "--loglevel=WARNING",
            f"--concurrency={CONCURRENCY}",
            f"--queues={','.join(queues)}",
            f"--hostname={SERVICE_NAME}@%h",
            "--without-gossip",
            "--without-mingle",
        ]
    )

    log(SERVICE_NAME, "stopping", topics=queues)


if __name__ == "__main__":
    main()
