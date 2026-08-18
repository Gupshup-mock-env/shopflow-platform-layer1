"""Celery configuration shared by every ShopFlow billing component.

Loaded by `billing.celery_app` through `config_from_object`, so the producer
and the worker always agree on serialization and routing.
"""

from __future__ import annotations

import os

_RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
_RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "guest")
_RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
_RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT", "5672")
_RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/")

broker_url = (
    f"amqp://{_RABBITMQ_USER}:{_RABBITMQ_PASSWORD}"
    f"@{_RABBITMQ_HOST}:{_RABBITMQ_PORT}/{_RABBITMQ_VHOST.lstrip('/')}"
)
broker_connection_retry_on_startup = True
broker_transport_options = {
    "client_properties": {
        "connection_name": os.environ.get("SERVICE_NAME", "shopflow-billing"),
    },
}

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
task_ignore_result = True

task_acks_late = True
worker_prefetch_multiplier = 1
worker_send_task_events = False

timezone = "UTC"
enable_utc = True

task_default_queue = "celery"

# Invoicing is throttled independently of the rest of the billing workload, so
# it gets a queue of its own rather than sharing the default one.
task_routes = {
    "billing.tasks.generate_invoice": {"queue": "invoicing"},
}
