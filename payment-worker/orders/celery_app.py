"""Celery application shared by the ShopFlow order pipeline.

Both the API side (which enqueues work) and the worker side (which runs it)
import this module so that they agree on the task registry and the routing
table declared in ``celeryconfig``.
"""

from __future__ import annotations

import os
from typing import Final
from urllib.parse import quote

from celery import Celery


def broker_url() -> str:
    user = os.environ.get("RABBITMQ_USER", "guest")
    password = os.environ.get("RABBITMQ_PASSWORD", "guest")
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    port = os.environ.get("RABBITMQ_PORT", "5672")
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    return (
        f"amqp://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{quote(vhost, safe='')}"
    )


app: Final[Celery] = Celery("shopflow_orders")
app.config_from_object("celeryconfig")
app.conf.broker_url = broker_url()


def resolve_queue(task_name: str) -> str:
    """Return the queue ``task_name`` is routed to by the current config."""
    route = app.conf.task_routes.get(task_name) if app.conf.task_routes else None
    if isinstance(route, str):
        return route
    if isinstance(route, dict) and route.get("queue"):
        return str(route["queue"])
    return str(app.conf.task_default_queue)
