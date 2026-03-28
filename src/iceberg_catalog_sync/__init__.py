"""Iceberg Catalog Sync — sync table metadata between two Iceberg REST catalogs."""

from iceberg_catalog_sync.config import AppConfig, load_config
from iceberg_catalog_sync.events import (
    ChangeSet,
    build_changeset_from_rows,
    consume_events,
    save_cursor,
    setup_risingwave,
)
from iceberg_catalog_sync.reporting import SyncResult
from iceberg_catalog_sync.sync import sync_catalogs, sync_from_changeset

__all__ = [
    "AppConfig",
    "ChangeSet",
    "SyncResult",
    "build_changeset_from_rows",
    "consume_events",
    "load_config",
    "save_cursor",
    "setup_risingwave",
    "sync_catalogs",
    "sync_from_changeset",
]
