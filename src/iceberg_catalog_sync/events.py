"""CloudEvent consumer for event-driven partial sync via RisingWave.

Consumes CloudEvents published by Lakekeeper to RisingWave via its Events API,
using RisingWave subscriptions with cursor-based consumption. Builds a
deduplicated ChangeSet of affected namespaces and tables, which is then used
to trigger a targeted partial sync instead of a full namespace scan.

Flow:
  Lakekeeper → (Events API HTTP POST) → RisingWave table → subscription
  → iceberg-catalog-sync reads subscription via cursor → ChangeSet → partial sync
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from iceberg_catalog_sync.config import EventsConfig

logger = logging.getLogger("iceberg_catalog_sync")

# Lakekeeper CloudEvent types that indicate a table mutation
TABLE_EVENT_TYPES = frozenset(
    {
        "createTable",
        "registerTable",
        "updateTable",
        "dropTable",
        "renameTable",
    }
)

# Lakekeeper CloudEvent types that indicate a namespace mutation
NAMESPACE_EVENT_TYPES = frozenset(
    {
        "CreateNamespaceEvent",
        "DropNamespaceEvent",
        "UpdateNamespacePropertiesEvent",
    }
)


@dataclass
class ChangeSet:
    """Deduplicated set of changes extracted from CloudEvents.

    Groups affected tables by namespace. Used to drive partial sync —
    only the tables/namespaces in the changeset are synced, avoiding
    full namespace scans.
    """

    # namespace -> set of table names that changed
    _tables: dict[str, set[str]] = field(default_factory=dict)
    # namespaces that had namespace-level events (create, drop, properties)
    _namespace_events: set[str] = field(default_factory=set)
    # total raw events consumed (before dedup)
    raw_event_count: int = 0

    def add_table_change(self, namespace: str, table: str) -> None:
        self._tables.setdefault(namespace, set()).add(table)

    def add_namespace_change(self, namespace: str) -> None:
        self._namespace_events.add(namespace)
        self._tables.setdefault(namespace, set())

    @property
    def affected_namespaces(self) -> set[str]:
        return set(self._tables.keys()) | self._namespace_events

    def affected_tables(self, namespace: str) -> set[str]:
        return self._tables.get(namespace, set())

    def has_namespace_changes(self, namespace: str) -> bool:
        return namespace in self._namespace_events

    @property
    def is_empty(self) -> bool:
        return not self._tables and not self._namespace_events

    def __repr__(self) -> str:
        ns_count = len(self.affected_namespaces)
        table_count = sum(len(ts) for ts in self._tables.values())
        return (
            f"ChangeSet(namespaces={ns_count}, tables={table_count}, "
            f"raw_events={self.raw_event_count})"
        )


def _parse_event_row(
    row: dict, managed_namespaces: set[str]
) -> tuple[str | None, str | None, str | None]:
    """Extract (namespace, table_name, event_type) from a RisingWave subscription row.

    The row contains columns from the lakekeeper_events table plus the
    subscription metadata columns (op, rw_timestamp).

    Returns (None, None, None) if the event is not relevant.
    """
    event_type = row.get("type", "")
    namespace = row.get("namespace", "")

    if not namespace:
        return None, None, None

    if namespace not in managed_namespaces:
        logger.debug("Skipping event for unmanaged namespace: %s", namespace)
        return None, None, None

    if event_type in TABLE_EVENT_TYPES:
        table_name = row.get("name", "")
        if table_name:
            return namespace, table_name, event_type
        return None, None, None

    if event_type in NAMESPACE_EVENT_TYPES:
        return namespace, None, event_type

    logger.debug("Skipping unrecognized event type: %s", event_type)
    return None, None, None


def build_changeset_from_rows(
    rows: list[dict], managed_namespaces: set[str]
) -> ChangeSet:
    """Build a ChangeSet from RisingWave subscription rows."""
    changeset = ChangeSet()
    changeset.raw_event_count = len(rows)

    for row in rows:
        op = row.get("op", "")
        # Only process inserts (new events). UpdateDelete/Delete are not relevant.
        if op not in ("Insert", "UpdateInsert", 1, 3):
            continue

        namespace, table_name, event_type = _parse_event_row(row, managed_namespaces)
        if namespace is None:
            continue
        if table_name is not None:
            changeset.add_table_change(namespace, table_name)
        else:
            changeset.add_namespace_change(namespace)

    logger.info(
        "Built %s from %d raw events",
        changeset,
        changeset.raw_event_count,
    )
    return changeset


def _load_cursor_timestamp(cursor_path: str) -> int | None:
    """Load the last rw_timestamp from the cursor file."""
    path = Path(cursor_path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return data.get("last_rw_timestamp")
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read cursor file %s: %s", cursor_path, exc)
        return None


def _save_cursor_timestamp(cursor_path: str, timestamp: int) -> None:
    """Save the last rw_timestamp to the cursor file."""
    path = Path(cursor_path)
    path.write_text(json.dumps({"last_rw_timestamp": timestamp}))
    logger.debug("Saved cursor timestamp %d to %s", timestamp, cursor_path)


def consume_events(config: EventsConfig) -> tuple[list[dict], int | None]:
    """Consume pending events from RisingWave subscription.

    Uses cursor-based consumption with offset tracking via a local file.
    Returns (rows, latest_rw_timestamp) where rows are dicts with column values
    and latest_rw_timestamp is the highest rw_timestamp seen (for cursor persistence).

    Requires the `risingwave-py` package.
    """
    try:
        from risingwave import OutputFormat, RisingWave, RisingWaveConnOptions
    except ImportError:
        raise ImportError(
            "RisingWave support requires the 'risingwave-py' package. "
            "Install with: pip install risingwave-py"
        ) from None

    rw_config = config.risingwave
    rw = RisingWave(
        RisingWaveConnOptions.from_connection_info(
            host=rw_config.host,
            port=rw_config.port,
            user=rw_config.user,
            password=rw_config.password,
            database=rw_config.database,
        )
    )

    cursor_name = "ics_cursor"
    last_ts = _load_cursor_timestamp(rw_config.cursor_path)

    # Declare subscription cursor
    if last_ts is not None:
        since_clause = f"SINCE {last_ts}"
    else:
        since_clause = "SINCE begin()"

    rw.execute(
        f"DECLARE {cursor_name} SUBSCRIPTION CURSOR FOR "
        f"{rw_config.subscription_name} {since_clause}"
    )

    all_rows: list[dict] = []
    latest_timestamp: int | None = None
    remaining = config.max_events

    while remaining > 0:
        result = rw.fetch(
            f"FETCH NEXT FROM {cursor_name}",
            format=OutputFormat.DATACLASS,
        )
        if not result:
            break

        for row in result:
            row_dict = row if isinstance(row, dict) else dict(row)
            all_rows.append(row_dict)

            rw_ts = row_dict.get("rw_timestamp")
            if rw_ts is not None and (
                latest_timestamp is None or rw_ts > latest_timestamp
            ):
                latest_timestamp = rw_ts

        remaining -= len(result)

    logger.info(
        "Consumed %d events from RisingWave subscription '%s'",
        len(all_rows),
        rw_config.subscription_name,
    )

    # Persist cursor position
    if latest_timestamp is not None:
        _save_cursor_timestamp(rw_config.cursor_path, latest_timestamp)

    return all_rows, latest_timestamp
