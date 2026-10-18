"""Runtime configuration (12-factor; every value overridable via ``ATLAS_*`` env vars)."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from atlas.core.enums import FailureMode, ReconciliationMode


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ATLAS_", env_file=".env", extra="ignore")

    # --- app
    app_name: str = "atlas"
    environment: str = "dev"
    log_level: str = "INFO"
    log_json: bool = True

    # --- api
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_keys: list[str] = Field(default_factory=lambda: ["dev-operator-key"])
    """Static API keys for the MVP. Production should swap this for OIDC/JWT."""
    idempotency_ttl_seconds: int = 86400

    # --- database
    database_url: str = "postgresql+asyncpg://atlas:atlas@localhost:5432/atlas"
    db_echo: bool = False
    db_pool_size: int = 10

    # --- temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "atlas-orchestration"
    temporal_tls: bool = False

    # --- network adapters
    adapter_backend: str = "mock"  # mock | iosxr | junos (per-device vendor is resolved from inventory)
    mock_failure_mode: FailureMode = FailureMode.NONE
    mock_latency_ms: int = 50
    device_credentials_ref: str = "env"  # where to resolve credentials: env | k8s-secret | vault
    device_username: str = "atlas"
    device_password: SecretStr = SecretStr("")

    # --- resource pools
    bgp_asn: int = 65000
    rd_pool_start: int = 1000
    rd_pool_end: int = 1999
    rt_pool_start: int = 100
    rt_pool_end: int = 999
    vlan_pool_start: int = 100
    vlan_pool_end: int = 4000
    pe_ce_supernet: str = "10.255.0.0/16"  # /30s are carved from here

    # --- reconciliation
    reconciliation_mode: ReconciliationMode = ReconciliationMode.DETECT_ONLY

    # --- observability
    metrics_enabled: bool = True
    otel_enabled: bool = False
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    otel_service_name: str = "atlas"


@lru_cache
def get_settings() -> Settings:
    return Settings()
