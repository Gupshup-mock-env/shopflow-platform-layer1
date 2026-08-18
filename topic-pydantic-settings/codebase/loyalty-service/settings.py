"""Settings for the loyalty points publisher."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    service_name: str = "loyalty-service"
    kafka_bootstrap: str = "localhost:9092"
    points_topic: str = "shopflow.loyalty.points_earned"
    publish_interval_seconds: float = 2.0
    delivery_timeout_seconds: float = 10.0
    health_port: int = 8080


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
