"""Application configuration loaded from environment variables and `.env`."""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application settings.

    Values are read from environment variables (case-insensitive) and fall back
    to a `.env` file in the working directory, then to the defaults below.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Application
    app_name: str = "Real-Time Data Sync Platform"
    app_version: str = "0.1.0"
    environment: Literal["development", "staging", "production", "test"] = "development"
    debug: bool = False
    log_level: str = "INFO"

    # Database
    database_url: str = "postgresql+asyncpg://sync_user:sync_password@localhost:5432/sync_db"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_echo: bool = False

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_sync_events_topic: str = "sync_events"
    kafka_consumer_group: str = "sync_group"
    kafka_client_id: str = "sync-platform-api"
    # Upper bound on connect/publish time, so a dead broker can't hang requests.
    kafka_publish_timeout_ms: int = 5000

    # Background processing: run the Kafka -> transform -> DB worker inside the API
    # process. Set false to run it separately: python -m src.workers.sync_processor
    run_sync_processor: bool = True
    # Entities processed in parallel per Kafka batch (each holds one DB connection).
    sync_processor_concurrency: int = 10

    # Kafka security, for managed clusters (e.g. Aiven). Local docker-compose uses
    # PLAINTEXT. For SSL client certs, put the PEM files' paths in the three
    # KAFKA_SSL_* settings; for SASL, set the mechanism, username and password.
    kafka_security_protocol: Literal["PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT"] = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None  # e.g. SCRAM-SHA-256, PLAIN
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_ssl_cafile: str | None = None
    kafka_ssl_certfile: str | None = None
    kafka_ssl_keyfile: str | None = None

    # Observability
    tracing_enabled: bool = False
    otel_service_name: str = "sync-platform"
    # Jaeger v2 accepts OTLP/HTTP on 4318 (see src/observability/tracing.py).
    otlp_traces_endpoint: str = "http://localhost:4318/v1/traces"

    # Future phases (declared now so config stays in one place)
    redis_url: str = "redis://localhost:6379/0"

    def kafka_client_options(self) -> dict[str, object]:
        """Connection/security keyword arguments shared by every aiokafka client."""
        options: dict[str, object] = {
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "security_protocol": self.kafka_security_protocol,
        }
        if self.kafka_security_protocol in ("SSL", "SASL_SSL"):
            from aiokafka.helpers import create_ssl_context

            options["ssl_context"] = create_ssl_context(
                cafile=self.kafka_ssl_cafile,
                certfile=self.kafka_ssl_certfile,
                keyfile=self.kafka_ssl_keyfile,
            )
        if self.kafka_security_protocol.startswith("SASL"):
            options["sasl_mechanism"] = self.kafka_sasl_mechanism or "PLAIN"
            options["sasl_plain_username"] = self.kafka_sasl_username
            options["sasl_plain_password"] = self.kafka_sasl_password
        return options


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance (parsed once, then cached)."""
    return Settings()
