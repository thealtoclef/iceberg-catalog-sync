"""Core sync algorithm — compare source and destination catalogs, apply diffs.

Supports two modes:
- Full sync: scan all tables in all managed namespaces (pull-based)
- Partial sync: sync only specific tables identified by a ChangeSet (event-driven)
"""

from __future__ import annotations

import logging
import time

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.table import Table

from iceberg_catalog_sync.config import AppConfig, CatalogConfig
from iceberg_catalog_sync.events import ChangeSet
from iceberg_catalog_sync.log import setup_logging
from iceberg_catalog_sync.reporting import (
    ActionType,
    SyncAction,
    SyncError,
    SyncResult,
)
from iceberg_catalog_sync.retry import make_retry_decorator
from iceberg_catalog_sync.telemetry import setup_metrics, setup_tracing

logger = logging.getLogger("iceberg_catalog_sync")


def _make_catalog(catalog_config: CatalogConfig) -> Catalog:
    return load_catalog(
        catalog_config.name,
        **{
            "uri": catalog_config.uri,
            "warehouse": catalog_config.warehouse,
            **catalog_config.properties,
        },
    )


def _get_metadata_location(table: Table) -> str:
    return table.metadata_location


def sync_catalogs(config: AppConfig) -> SyncResult:
    """Run a full sync between source and destination catalogs.

    Stateless and idempotent — safe to restart at any point.
    """
    setup_logging(config.log)
    tracer = setup_tracing(config.tracing)
    meter = setup_metrics(config.metrics)

    # Set up metrics counters
    tables_registered = meter.create_counter(
        "sync.tables.registered", description="Tables newly registered in destination"
    )
    tables_updated = meter.create_counter(
        "sync.tables.updated",
        description="Tables with updated metadata location",
    )
    tables_dropped = meter.create_counter(
        "sync.tables.dropped", description="Orphan tables dropped from destination"
    )
    tables_up_to_date = meter.create_counter(
        "sync.tables.up_to_date", description="Tables already in sync"
    )
    tables_errors = meter.create_counter(
        "sync.tables.errors", description="Per-table sync failures"
    )
    namespaces_created = meter.create_counter(
        "sync.namespaces.created", description="Namespaces created in destination"
    )
    sync_duration = meter.create_histogram(
        "sync.duration_seconds", description="Total sync duration"
    )
    retries_counter = meter.create_counter(
        "sync.retries", description="Retry attempts due to rate limits"
    )

    retry_decorator = make_retry_decorator(
        config.retry, retries_counter=retries_counter
    )

    result = SyncResult(dry_run=config.sync.dry_run)
    start_time = time.monotonic()

    with tracer.start_as_current_span("sync_catalogs") as root_span:
        root_span.set_attribute("dry_run", config.sync.dry_run)

        source = _make_catalog(config.catalogs.source)
        dest = _make_catalog(config.catalogs.destination)

        for namespace_str in config.sync.namespaces:
            namespace = tuple(namespace_str.split("."))
            with tracer.start_as_current_span(
                f"sync_namespace({namespace_str})"
            ) as ns_span:
                ns_span.set_attribute("namespace", namespace_str)

                # 1. Ensure namespace exists in destination
                try:
                    _sync_namespace(
                        source=source,
                        dest=dest,
                        namespace=namespace,
                        config=config,
                        result=result,
                        retry_decorator=retry_decorator,
                        namespaces_created=namespaces_created,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to sync namespace %s: %s", namespace_str, exc
                    )
                    result.errors.append(
                        SyncError(namespace=namespace_str, error=str(exc))
                    )
                    continue

                # 2. Sync tables
                try:
                    _sync_tables(
                        source=source,
                        dest=dest,
                        namespace=namespace,
                        namespace_str=namespace_str,
                        config=config,
                        result=result,
                        tracer=tracer,
                        retry_decorator=retry_decorator,
                        tables_registered=tables_registered,
                        tables_updated=tables_updated,
                        tables_dropped=tables_dropped,
                        tables_up_to_date=tables_up_to_date,
                        tables_errors=tables_errors,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to sync tables in namespace %s: %s",
                        namespace_str,
                        exc,
                    )
                    result.errors.append(
                        SyncError(namespace=namespace_str, error=str(exc))
                    )

    elapsed = time.monotonic() - start_time
    sync_duration.record(elapsed)
    logger.info("Sync completed in %.2fs: %s", elapsed, result.summary)
    return result


def sync_from_changeset(config: AppConfig, changeset: ChangeSet) -> SyncResult:
    """Run a partial sync — only sync tables/namespaces identified by the ChangeSet.

    Instead of listing all tables in each namespace (expensive), this loads
    and compares only the specific tables that changed according to events.
    This dramatically reduces API calls to both source and destination catalogs.
    """
    setup_logging(config.log)
    tracer = setup_tracing(config.tracing)
    meter = setup_metrics(config.metrics)

    tables_registered = meter.create_counter(
        "sync.tables.registered", description="Tables newly registered in destination"
    )
    tables_updated = meter.create_counter(
        "sync.tables.updated", description="Tables with updated metadata location"
    )
    tables_dropped = meter.create_counter(
        "sync.tables.dropped", description="Orphan tables dropped from destination"
    )
    tables_up_to_date = meter.create_counter(
        "sync.tables.up_to_date", description="Tables already in sync"
    )
    tables_errors = meter.create_counter(
        "sync.tables.errors", description="Per-table sync failures"
    )
    namespaces_created = meter.create_counter(
        "sync.namespaces.created", description="Namespaces created in destination"
    )
    sync_duration = meter.create_histogram(
        "sync.duration_seconds", description="Total sync duration"
    )
    retries_counter = meter.create_counter(
        "sync.retries", description="Retry attempts due to rate limits"
    )

    retry_decorator = make_retry_decorator(
        config.retry, retries_counter=retries_counter
    )

    result = SyncResult(dry_run=config.sync.dry_run)
    managed = set(config.sync.namespaces)
    start_time = time.monotonic()

    with tracer.start_as_current_span("sync_from_changeset") as root_span:
        root_span.set_attribute("dry_run", config.sync.dry_run)
        root_span.set_attribute("mode", "events")

        source = _make_catalog(config.catalogs.source)
        dest = _make_catalog(config.catalogs.destination)

        for namespace_str in changeset.affected_namespaces & managed:
            namespace = tuple(namespace_str.split("."))
            with tracer.start_as_current_span(
                f"sync_namespace({namespace_str})"
            ) as ns_span:
                ns_span.set_attribute("namespace", namespace_str)

                # Ensure namespace exists in destination
                try:
                    _sync_namespace(
                        source=source,
                        dest=dest,
                        namespace=namespace,
                        config=config,
                        result=result,
                        retry_decorator=retry_decorator,
                        namespaces_created=namespaces_created,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to sync namespace %s: %s", namespace_str, exc
                    )
                    result.errors.append(
                        SyncError(namespace=namespace_str, error=str(exc))
                    )
                    continue

                # Sync only the specific tables from the changeset
                for table_name in changeset.affected_tables(namespace_str):
                    try:
                        _sync_single_table(
                            source=source,
                            dest=dest,
                            namespace=namespace,
                            namespace_str=namespace_str,
                            table_name=table_name,
                            config=config,
                            result=result,
                            tracer=tracer,
                            retry_decorator=retry_decorator,
                            tables_registered=tables_registered,
                            tables_updated=tables_updated,
                            tables_dropped=tables_dropped,
                            tables_up_to_date=tables_up_to_date,
                            tables_errors=tables_errors,
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to sync table %s.%s: %s",
                            namespace_str,
                            table_name,
                            exc,
                        )
                        result.errors.append(
                            SyncError(
                                namespace=namespace_str,
                                table=table_name,
                                error=str(exc),
                            )
                        )
                        tables_errors.add(1, {"namespace": namespace_str})

    elapsed = time.monotonic() - start_time
    sync_duration.record(elapsed)
    logger.info("Partial sync completed in %.2fs: %s", elapsed, result.summary)
    return result


def _sync_single_table(
    *,
    source: Catalog,
    dest: Catalog,
    namespace: tuple[str, ...],
    namespace_str: str,
    table_name: str,
    config: AppConfig,
    result: SyncResult,
    tracer,
    retry_decorator,
    tables_registered,
    tables_updated,
    tables_dropped,
    tables_up_to_date,
    tables_errors,
) -> None:
    """Sync a single table by loading it from source and dest, then applying the diff."""
    table_id = (*namespace, table_name)

    # Load table from source (may not exist if it was dropped)
    source_table: Table | None = None
    try:
        source_table = source.load_table(table_id)
    except NoSuchTableError:
        pass
    except Exception as exc:
        logger.error("Failed to load source table %s.%s: %s", namespace_str, table_name, exc)
        result.errors.append(
            SyncError(namespace=namespace_str, table=table_name, error=str(exc))
        )
        tables_errors.add(1, {"namespace": namespace_str})
        return

    # Load table from dest (may not exist yet)
    dest_table: Table | None = None
    try:
        dest_table = dest.load_table(table_id)
    except NoSuchTableError:
        pass
    except Exception as exc:
        logger.error("Failed to load dest table %s.%s: %s", namespace_str, table_name, exc)
        result.errors.append(
            SyncError(namespace=namespace_str, table=table_name, error=str(exc))
        )
        tables_errors.add(1, {"namespace": namespace_str})
        return

    if source_table is not None and dest_table is None:
        # Table exists in source only → register
        metadata_location = _get_metadata_location(source_table)
        with tracer.start_as_current_span("register_table") as span:
            span.set_attribute("namespace", namespace_str)
            span.set_attribute("table_id", table_name)
            span.set_attribute("metadata_location", metadata_location)
            _register_table(
                dest=dest,
                namespace=namespace,
                namespace_str=namespace_str,
                table_name=table_name,
                metadata_location=metadata_location,
                config=config,
                result=result,
                retry_decorator=retry_decorator,
                counter=tables_registered,
            )

    elif source_table is not None and dest_table is not None:
        # Both exist → check metadata location
        source_loc = _get_metadata_location(source_table)
        dest_loc = _get_metadata_location(dest_table)
        if source_loc == dest_loc:
            logger.debug("Table %s.%s is up to date", namespace_str, table_name)
            result.actions.append(
                SyncAction(
                    action=ActionType.SKIP,
                    namespace=namespace_str,
                    table=table_name,
                    metadata_location=source_loc,
                )
            )
            tables_up_to_date.add(1, {"namespace": namespace_str})
        else:
            with tracer.start_as_current_span("update_table") as span:
                span.set_attribute("namespace", namespace_str)
                span.set_attribute("table_id", table_name)
                span.set_attribute("metadata_location", source_loc)
                _update_table(
                    dest=dest,
                    namespace=namespace,
                    namespace_str=namespace_str,
                    table_name=table_name,
                    metadata_location=source_loc,
                    config=config,
                    result=result,
                    retry_decorator=retry_decorator,
                    counter=tables_updated,
                )

    elif source_table is None and dest_table is not None:
        # Table gone from source, still in dest → drop if configured
        if config.sync.drop_orphan_tables:
            with tracer.start_as_current_span("drop_table") as span:
                span.set_attribute("namespace", namespace_str)
                span.set_attribute("table_id", table_name)
                _drop_orphan_table(
                    dest=dest,
                    namespace=namespace,
                    namespace_str=namespace_str,
                    table_name=table_name,
                    config=config,
                    result=result,
                    retry_decorator=retry_decorator,
                    counter=tables_dropped,
                )
        else:
            logger.warning(
                "Table %s.%s gone from source but drop_orphan_tables=false, skipping",
                namespace_str,
                table_name,
            )
    else:
        # Neither exists — already processed or no-op
        logger.debug(
            "Table %s.%s not found in source or dest, skipping",
            namespace_str,
            table_name,
        )


def _sync_namespace(
    *,
    source: Catalog,
    dest: Catalog,
    namespace: tuple[str, ...],
    config: AppConfig,
    result: SyncResult,
    retry_decorator,
    namespaces_created,
) -> None:
    namespace_str = ".".join(namespace)

    # Check if namespace exists in destination
    try:
        dest.load_namespace_properties(namespace)
        ns_exists = True
    except NoSuchNamespaceError:
        ns_exists = False

    if not ns_exists:
        source_properties = source.load_namespace_properties(namespace)
        if config.sync.dry_run:
            logger.info("[DRY RUN] Would create namespace %s", namespace_str)
            result.actions.append(
                SyncAction(
                    action=ActionType.CREATE_NAMESPACE,
                    namespace=namespace_str,
                    dry_run=True,
                )
            )
        else:
            logger.info("Creating namespace %s in destination", namespace_str)
            try:
                retry_decorator(dest.create_namespace)(namespace, source_properties)
            except NamespaceAlreadyExistsError:
                logger.debug(
                    "Namespace %s already exists (race condition), continuing",
                    namespace_str,
                )
            result.actions.append(
                SyncAction(
                    action=ActionType.CREATE_NAMESPACE, namespace=namespace_str
                )
            )
            namespaces_created.add(1)
    elif config.sync.sync_namespace_properties:
        _sync_namespace_properties(
            source=source,
            dest=dest,
            namespace=namespace,
            namespace_str=namespace_str,
            config=config,
            result=result,
            retry_decorator=retry_decorator,
        )


def _sync_namespace_properties(
    *,
    source: Catalog,
    dest: Catalog,
    namespace: tuple[str, ...],
    namespace_str: str,
    config: AppConfig,
    result: SyncResult,
    retry_decorator,
) -> None:
    source_props = source.load_namespace_properties(namespace)
    dest_props = dest.load_namespace_properties(namespace)

    # Additive only — add properties from source that are missing in dest
    new_props = {k: v for k, v in source_props.items() if k not in dest_props}
    if not new_props:
        return

    if config.sync.dry_run:
        logger.info(
            "[DRY RUN] Would update namespace properties for %s: %s",
            namespace_str,
            list(new_props.keys()),
        )
        result.actions.append(
            SyncAction(
                action=ActionType.UPDATE_NAMESPACE_PROPERTIES,
                namespace=namespace_str,
                dry_run=True,
            )
        )
    else:
        logger.info(
            "Updating namespace properties for %s: %s",
            namespace_str,
            list(new_props.keys()),
        )
        retry_decorator(dest.update_namespace_properties)(
            namespace, removals=set(), updates=new_props
        )
        result.actions.append(
            SyncAction(
                action=ActionType.UPDATE_NAMESPACE_PROPERTIES,
                namespace=namespace_str,
            )
        )


def _sync_tables(
    *,
    source: Catalog,
    dest: Catalog,
    namespace: tuple[str, ...],
    namespace_str: str,
    config: AppConfig,
    result: SyncResult,
    tracer,
    retry_decorator,
    tables_registered,
    tables_updated,
    tables_dropped,
    tables_up_to_date,
    tables_errors,
) -> None:
    source_table_ids = source.list_tables(namespace)
    dest_table_ids = dest.list_tables(namespace)

    source_names = {table_id[-1] for table_id in source_table_ids}
    dest_names = {table_id[-1] for table_id in dest_table_ids}

    # Build metadata location maps
    source_tables: dict[str, Table] = {}
    for table_id in source_table_ids:
        table_name = table_id[-1]
        try:
            source_tables[table_name] = source.load_table(table_id)
        except Exception as exc:
            logger.error("Failed to load source table %s.%s: %s", namespace_str, table_name, exc)
            result.errors.append(
                SyncError(namespace=namespace_str, table=table_name, error=str(exc))
            )
            tables_errors.add(1, {"namespace": namespace_str})

    dest_table_map: dict[str, Table] = {}
    for table_id in dest_table_ids:
        table_name = table_id[-1]
        try:
            dest_table_map[table_name] = dest.load_table(table_id)
        except Exception as exc:
            logger.error("Failed to load dest table %s.%s: %s", namespace_str, table_name, exc)
            result.errors.append(
                SyncError(namespace=namespace_str, table=table_name, error=str(exc))
            )
            tables_errors.add(1, {"namespace": namespace_str})

    # Tables in source only → register
    for table_name in source_names - dest_names:
        if table_name not in source_tables:
            continue
        source_table = source_tables[table_name]
        metadata_location = _get_metadata_location(source_table)

        with tracer.start_as_current_span("register_table") as span:
            span.set_attribute("namespace", namespace_str)
            span.set_attribute("table_id", table_name)
            span.set_attribute("metadata_location", metadata_location)

            try:
                _register_table(
                    dest=dest,
                    namespace=namespace,
                    namespace_str=namespace_str,
                    table_name=table_name,
                    metadata_location=metadata_location,
                    config=config,
                    result=result,
                    retry_decorator=retry_decorator,
                    counter=tables_registered,
                )
            except Exception as exc:
                logger.error(
                    "Failed to register table %s.%s: %s",
                    namespace_str,
                    table_name,
                    exc,
                )
                result.errors.append(
                    SyncError(
                        namespace=namespace_str, table=table_name, error=str(exc)
                    )
                )
                tables_errors.add(1, {"namespace": namespace_str})

    # Tables in both → check metadata location
    for table_name in source_names & dest_names:
        if table_name not in source_tables or table_name not in dest_table_map:
            continue
        source_loc = _get_metadata_location(source_tables[table_name])
        dest_loc = _get_metadata_location(dest_table_map[table_name])

        if source_loc == dest_loc:
            logger.debug("Table %s.%s is up to date", namespace_str, table_name)
            result.actions.append(
                SyncAction(
                    action=ActionType.SKIP,
                    namespace=namespace_str,
                    table=table_name,
                    metadata_location=source_loc,
                )
            )
            tables_up_to_date.add(1, {"namespace": namespace_str})
        else:
            with tracer.start_as_current_span("update_table") as span:
                span.set_attribute("namespace", namespace_str)
                span.set_attribute("table_id", table_name)
                span.set_attribute("metadata_location", source_loc)

                try:
                    _update_table(
                        dest=dest,
                        namespace=namespace,
                        namespace_str=namespace_str,
                        table_name=table_name,
                        metadata_location=source_loc,
                        config=config,
                        result=result,
                        retry_decorator=retry_decorator,
                        counter=tables_updated,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to update table %s.%s: %s",
                        namespace_str,
                        table_name,
                        exc,
                    )
                    result.errors.append(
                        SyncError(
                            namespace=namespace_str, table=table_name, error=str(exc)
                        )
                    )
                    tables_errors.add(1, {"namespace": namespace_str})

    # Tables in dest only → drop or skip
    for table_name in dest_names - source_names:
        if config.sync.drop_orphan_tables:
            with tracer.start_as_current_span("drop_table") as span:
                span.set_attribute("namespace", namespace_str)
                span.set_attribute("table_id", table_name)

                try:
                    _drop_orphan_table(
                        dest=dest,
                        namespace=namespace,
                        namespace_str=namespace_str,
                        table_name=table_name,
                        config=config,
                        result=result,
                        retry_decorator=retry_decorator,
                        counter=tables_dropped,
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to drop orphan table %s.%s: %s",
                        namespace_str,
                        table_name,
                        exc,
                    )
                    result.errors.append(
                        SyncError(
                            namespace=namespace_str, table=table_name, error=str(exc)
                        )
                    )
                    tables_errors.add(1, {"namespace": namespace_str})
        else:
            logger.warning(
                "Orphan table %s.%s in destination, skipping (drop_orphan_tables=false)",
                namespace_str,
                table_name,
            )


def _register_table(
    *,
    dest: Catalog,
    namespace: tuple[str, ...],
    namespace_str: str,
    table_name: str,
    metadata_location: str,
    config: AppConfig,
    result: SyncResult,
    retry_decorator,
    counter,
) -> None:
    table_id = (*namespace, table_name)
    if config.sync.dry_run:
        logger.info(
            "[DRY RUN] Would register table %s.%s -> %s",
            namespace_str,
            table_name,
            metadata_location,
        )
        result.actions.append(
            SyncAction(
                action=ActionType.REGISTER,
                namespace=namespace_str,
                table=table_name,
                metadata_location=metadata_location,
                dry_run=True,
            )
        )
    else:
        logger.info(
            "Registering table %s.%s -> %s",
            namespace_str,
            table_name,
            metadata_location,
        )
        try:
            retry_decorator(dest.register_table)(table_id, metadata_location)
        except TableAlreadyExistsError:
            logger.debug(
                "Table %s.%s already exists (race condition), will update",
                namespace_str,
                table_name,
            )
            _update_table(
                dest=dest,
                namespace=namespace,
                namespace_str=namespace_str,
                table_name=table_name,
                metadata_location=metadata_location,
                config=config,
                result=result,
                retry_decorator=retry_decorator,
                counter=counter,
            )
            return
        result.actions.append(
            SyncAction(
                action=ActionType.REGISTER,
                namespace=namespace_str,
                table=table_name,
                metadata_location=metadata_location,
            )
        )
        counter.add(1, {"namespace": namespace_str})


def _update_table(
    *,
    dest: Catalog,
    namespace: tuple[str, ...],
    namespace_str: str,
    table_name: str,
    metadata_location: str,
    config: AppConfig,
    result: SyncResult,
    retry_decorator,
    counter,
) -> None:
    table_id = (*namespace, table_name)
    if config.sync.dry_run:
        logger.info(
            "[DRY RUN] Would update table %s.%s -> %s",
            namespace_str,
            table_name,
            metadata_location,
        )
        result.actions.append(
            SyncAction(
                action=ActionType.UPDATE,
                namespace=namespace_str,
                table=table_name,
                metadata_location=metadata_location,
                dry_run=True,
            )
        )
    else:
        logger.info(
            "Updating table %s.%s -> %s (drop + register)",
            namespace_str,
            table_name,
            metadata_location,
        )
        try:
            retry_decorator(dest.drop_table)(table_id)
        except NoSuchTableError:
            pass
        retry_decorator(dest.register_table)(table_id, metadata_location)
        result.actions.append(
            SyncAction(
                action=ActionType.UPDATE,
                namespace=namespace_str,
                table=table_name,
                metadata_location=metadata_location,
            )
        )
        counter.add(1, {"namespace": namespace_str})


def _drop_orphan_table(
    *,
    dest: Catalog,
    namespace: tuple[str, ...],
    namespace_str: str,
    table_name: str,
    config: AppConfig,
    result: SyncResult,
    retry_decorator,
    counter,
) -> None:
    table_id = (*namespace, table_name)
    if config.sync.dry_run:
        logger.info(
            "[DRY RUN] Would drop orphan table %s.%s",
            namespace_str,
            table_name,
        )
        result.actions.append(
            SyncAction(
                action=ActionType.DROP,
                namespace=namespace_str,
                table=table_name,
                dry_run=True,
            )
        )
    else:
        logger.info("Dropping orphan table %s.%s", namespace_str, table_name)
        try:
            retry_decorator(dest.drop_table)(table_id)
        except NoSuchTableError:
            pass
        result.actions.append(
            SyncAction(
                action=ActionType.DROP,
                namespace=namespace_str,
                table=table_name,
            )
        )
        counter.add(1, {"namespace": namespace_str})
