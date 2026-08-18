"""Environment-derived settings for the alerting services."""

from __future__ import annotations

import os
from urllib.parse import quote


def service_name(default: str) -> str:
    return os.environ.get("SERVICE_NAME", default)


def health_port() -> int:
    return int(os.environ.get("HEALTH_PORT", "8080"))


def rabbitmq_url() -> str:
    user = os.environ.get("RABBITMQ_USER", "guest")
    password = os.environ.get("RABBITMQ_PASSWORD", "guest")
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    port = os.environ.get("RABBITMQ_PORT", "5672")
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    return (
        f"amqp://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{quote(vhost, safe='')}"
    )
