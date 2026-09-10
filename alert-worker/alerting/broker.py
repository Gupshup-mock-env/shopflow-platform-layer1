"""Dramatiq broker wiring shared by the alerting services."""

from __future__ import annotations

from typing import Final

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import CurrentMessage

from .config import health_port, rabbitmq_url, service_name
from .observability import ObservabilityMiddleware

SERVICE_NAME: Final[str] = service_name("alert-worker")

broker: Final[RabbitmqBroker] = RabbitmqBroker(
    url=rabbitmq_url(),
    confirm_delivery=True,
)
broker.add_middleware(CurrentMessage())
broker.add_middleware(ObservabilityMiddleware(SERVICE_NAME, health_port()))

dramatiq.set_broker(broker)
