"""Shared fixtures for tests — mock catalog factories."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest

from iceberg_catalog_sync.config import (
    AppConfig,
    CatalogConfig,
    CatalogsConfig,
    LogConfig,
    RetryConfig,
    SyncBehaviorConfig,
)


def make_mock_table(metadata_location: str) -> MagicMock:
    """Create a mock Table with a given metadata_location."""
    table = MagicMock()
    type(table).metadata_location = PropertyMock(return_value=metadata_location)
    return table


@pytest.fixture
def base_config() -> AppConfig:
    return AppConfig(
        catalogs=CatalogsConfig(
            source=CatalogConfig(
                name="source", uri="http://source:8181", warehouse="wh"
            ),
            destination=CatalogConfig(
                name="dest", uri="http://dest:8181", warehouse="wh"
            ),
        ),
        sync=SyncBehaviorConfig(),
        retry=RetryConfig(max_attempts=1),
        log=LogConfig(level="DEBUG"),
    )
