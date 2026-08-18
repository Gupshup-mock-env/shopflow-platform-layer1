"""Kafka transport for identity events."""

from __future__ import annotations

from typing import Callable
from uuid import uuid4

from confluent_kafka import Message, Producer

from models import UserRegisteredEvent

LogFn = Callable[..., None]


class UserEventPublisher:
    """Publishes identity events for the rest of the platform.

    The instance owns both the destination it writes to and the underlying
    ``confluent_kafka.Producer``, so callers never touch client configuration.
    """

    def __init__(self, bootstrap_servers: str, client_id: str, log: LogFn) -> None:
        self.topic = "shopflow.users.registered"
        self.client_id = client_id
        self._log = log
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": f"{client_id}-0",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
                "retries": 5,
            }
        )

    def list_topics(self, timeout: float = 5.0) -> object:
        """Metadata round trip, used as the broker readiness check."""
        return self._producer.list_topics(timeout=timeout)

    def _on_delivery(self, err: object, msg: Message) -> None:
        if err is not None:
            self._log(self.client_id, "delivery_failed", topic=msg.topic(), error=str(err))
            return
        self._log(
            self.client_id,
            "delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )

    def publish(self, event: UserRegisteredEvent) -> str:
        message_id = str(uuid4())
        self._producer.produce(
            topic=self.topic,
            key=event.user_id.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
            headers=[
                ("content-type", b"application/json"),
                ("event-type", b"user.registered"),
                ("message-id", message_id.encode("utf-8")),
            ],
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)
        self._log(
            self.client_id,
            "published",
            topic=self.topic,
            message_id=message_id,
            user_id=event.user_id,
        )
        return message_id

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
