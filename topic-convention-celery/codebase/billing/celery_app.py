"""The Celery application every billing component shares."""

from __future__ import annotations

from celery import Celery

app = Celery("shopflow.billing", include=["billing.tasks"])
app.config_from_object("celeryconfig")


def destination_queue(task_name: str) -> str:
    """Resolve the queue a task is routed to by the configured task routes."""
    route = app.amqp.router.route({}, task_name) or {}
    queue = route.get("queue")
    return getattr(queue, "name", app.conf.task_default_queue)


def routed_queues() -> list[str]:
    """Every queue the configured routes point at, for the worker to serve."""
    routes = app.conf.task_routes or {}
    queues = {
        route["queue"]
        for route in routes.values()
        if isinstance(route, dict) and "queue" in route
    }
    return sorted(queues) or [app.conf.task_default_queue]
