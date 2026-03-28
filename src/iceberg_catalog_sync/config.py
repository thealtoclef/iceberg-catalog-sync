"""Pydantic v2 configuration models and YAML loader with environment variable expansion."""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator


class CatalogConfig(BaseModel):
    name: str
    uri: str
    warehouse: str
    properties: dict[str, str] = Field(default_factory=dict)


class CatalogsConfig(BaseModel):
    source: CatalogConfig
    destination: CatalogConfig


class SyncBehaviorConfig(BaseModel):
    exclude_namespaces: list[str] = Field(default_factory=list)
    dry_run: bool = False
    drop_orphan_tables: bool = False
    sync_namespace_properties: bool = True


class RetryConfig(BaseModel):
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0


class LogConfig(BaseModel):
    level: str = "INFO"
    format: str = "text"

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        v = v.upper()
        if v not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(f"Invalid log level: {v}")
        return v

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in ("text", "json"):
            raise ValueError(f"Invalid log format: {v}, must be 'text' or 'json'")
        return v


class MetricsConfig(BaseModel):
    enabled: bool = False
    exporter: str = "otlp"
    otlp_endpoint: str = "http://localhost:4317"


class TracingConfig(BaseModel):
    enabled: bool = False
    exporter: str = "otlp"
    otlp_endpoint: str = "http://localhost:4317"
    service_name: str = "iceberg-catalog-sync"


class RisingWaveConfig(BaseModel):
    host: str = "localhost"
    port: int = 4566
    user: str = "root"
    password: str = "root"
    database: str = "dev"
    schema_name: str = "public"
    source_table: str = "lakekeeper_events_raw"
    retention: str = "1 day"

    @property
    def mv_name(self) -> str:
        return f"__ics_{self.source_table}_mv"

    @property
    def subscription_name(self) -> str:
        return f"__ics_{self.source_table}_sub"

    @property
    def progress_table(self) -> str:
        return f"__ics_{self.source_table}_progress"


class EventsConfig(BaseModel):
    enabled: bool = False
    risingwave: RisingWaveConfig = RisingWaveConfig()
    sync_interval_seconds: int = 60
    max_events: int = 1000


class AppConfig(BaseModel):
    """Root config — maps 1:1 to YAML structure."""

    catalogs: CatalogsConfig
    sync: SyncBehaviorConfig = SyncBehaviorConfig()
    retry: RetryConfig = RetryConfig()
    log: LogConfig = LogConfig()
    events: EventsConfig = EventsConfig()
    metrics: MetricsConfig = MetricsConfig()
    tracing: TracingConfig = TracingConfig()


_ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+?)(?::-(.*?))?\}")


def _expand_env_vars(value: str) -> str:
    """Expand ${ENV_VAR} and ${ENV_VAR:-default} in a string."""

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2)
        env_value = os.environ.get(var_name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        raise ValueError(
            f"Environment variable '{var_name}' is not set and no default provided"
        )

    return _ENV_VAR_PATTERN.sub(replacer, value)


def _expand_env_recursive(data: object) -> object:
    """Recursively expand environment variables in a data structure."""
    if isinstance(data, str):
        return _expand_env_vars(data)
    if isinstance(data, dict):
        return {k: _expand_env_recursive(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_expand_env_recursive(item) for item in data]
    return data


def load_config(path: str | Path) -> AppConfig:
    """Load YAML config with ${ENV_VAR} and ${ENV_VAR:-default} expansion."""
    with open(path) as f:
        data = yaml.safe_load(f)
    data = _expand_env_recursive(data)
    return AppConfig(**data)
