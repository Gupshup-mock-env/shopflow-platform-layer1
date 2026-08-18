"""Kafka transport for the profile projection."""

from __future__ import annotations

from confluent_kafka import Consumer, Message


class UserEventConsumer:
    """Reads identity events for the profile projection.

    The instance owns the source it reads from and the underlying
    ``confluent_kafka.Consumer``, so the projection code stays transport free.
    """

    def __init__(self, bootstrap_servers: str, consumer_group: str, client_id: str) -> None:
        self.topic = "shopflow.users.registered"
        self.consumer_group = consumer_group
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": consumer_group,
                "client.id": client_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
            }
        )

    def list_topics(self, timeout: float = 5.0) -> object:
        """Metadata round trip, used as the broker readiness check."""
        return self._consumer.list_topics(timeout=timeout)

    def subscribe(self) -> None:
        self._consumer.subscribe([self.topic])

    def poll(self, timeout: float = 1.0) -> Message | None:
        return self._consumer.poll(timeout)

    def commit(self, msg: Message) -> None:
        self._consumer.commit(message=msg, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
