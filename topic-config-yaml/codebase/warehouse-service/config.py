"""Loader for the ShopFlow inventory platform configuration file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_FALLBACK_PATHS: tuple[Path, ...] = (
    Path("/etc/shopflow/config.yaml"),
    Path(__file__).resolve().parent.parent / "config.yaml",
)


class ConfigError(RuntimeError):
    """Raised when the configuration file is missing or incomplete."""


@dataclass(frozen=True)
class ConsumerConfig:
    group_id: str
    auto_offset_reset: str
    poll_timeout_seconds: float


@dataclass(frozen=True)
class AppConfig:
    path: Path
    domain: str
    health_port: int
    publish_interval_seconds: float
    delivery_timeout_seconds: float
    topics: dict[str, str]
    consumers: dict[str, ConsumerConfig]

    def topic(self, key: str) -> str:
        try:
            return self.topics[key]
        except KeyError:
            raise ConfigError(f"messaging.topics.{key} is not defined in {self.path}") from None

    def consumer(self, name: str) -> ConsumerConfig:
        try:
            return self.consumers[name]
        except KeyError:
            raise ConfigError(f"consumers.{name} is not defined in {self.path}") from None


def resolve_config_path() -> Path:
    override = os.environ.get("SHOPFLOW_CONFIG_PATH")
    if override:
        return Path(override)
    for candidate in _FALLBACK_PATHS:
        if candidate.is_file():
            return candidate
    return _FALLBACK_PATHS[-1]


def load_config() -> AppConfig:
    path = resolve_config_path()
    if not path.is_file():
        raise ConfigError(f"configuration file not found at {path}")
    with path.open("r", encoding="utf-8") as handle:
        document: dict[str, Any] = yaml.safe_load(handle) or {}

    service = document.get("service") or {}
    messaging = document.get("messaging") or {}
    topics = {str(key): str(value) for key, value in (messaging.get("topics") or {}).items()}

    consumers: dict[str, ConsumerConfig] = {}
    for name, settings in (document.get("consumers") or {}).items():
        settings = settings or {}
        consumers[str(name)] = ConsumerConfig(
            group_id=str(settings.get("group_id", name)),
            auto_offset_reset=str(settings.get("auto_offset_reset", "earliest")),
            poll_timeout_seconds=float(settings.get("poll_timeout_seconds", 1.0)),
        )

    return AppConfig(
        path=path,
        domain=str(service.get("domain", "inventory")),
        health_port=int(service.get("health_port", 8080)),
        publish_interval_seconds=float(messaging.get("publish_interval_seconds", 2)),
        delivery_timeout_seconds=float(messaging.get("delivery_timeout_seconds", 10)),
        topics=topics,
        consumers=consumers,
    )
