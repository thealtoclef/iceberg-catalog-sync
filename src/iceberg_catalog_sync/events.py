"""CloudEvent consumer for event-driven partial sync via RisingWave.

Consumes CloudEvents published by Lakekeeper to RisingWave via its webhook
source. Uses RisingWave subscriptions (cursor-based) per
https://docs.risingwave.com/serve/subscription to read changes, then builds
a deduplicated ChangeSet of affected namespaces and tables for partial sync.

Connects to RisingWave via psycopg2 (Postgres wire protocol). The only
prerequisite is the webhook source table (single JSONB column) — this module
manages all other RisingWave objects:

  - Materialized view:  __ics_{source_table}_mv       (flattens JSONB)
  - Subscription:       __ics_{source_table}_sub       (change feed)
  - Progress table:     __ics_{source_table}_progress  (cursor tracking)

All derived from the single ``source_table`` config input, prefixed with
``__ics_`` to signal internal/private objects.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from iceberg_catalog_sync.config import EventsConfig, RisingWaveConfig

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
    row: dict,
    exclude_namespaces: set[str] | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Extract (namespace, table_name, event_type) from a RisingWave subscription row.

    Returns (None, None, None) if the event is not relevant.
    """
    event_type = row.get("type", "")
    namespace = row.get("namespace", "")

    if not namespace:
        return None, None, None

    if exclude_namespaces and namespace in exclude_namespaces:
        logger.debug("Skipping event for excluded namespace: %s", namespace)
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
    rows: list[dict],
    exclude_namespaces: set[str] | None = None,
) -> ChangeSet:
    """Build a ChangeSet from RisingWave subscription rows."""
    changeset = ChangeSet()
    changeset.raw_event_count = len(rows)

    for row in rows:
        op = row.get("op", "")
        # Only process inserts (new events). UpdateDelete/Delete are not relevant.
        if op not in ("Insert", "UpdateInsert", 1, 3):
            continue

        namespace, table_name, event_type = _parse_event_row(
            row, exclude_namespaces=exclude_namespaces
        )
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


# ── RisingWave connection and SQL helpers ──


def _connect(config: EventsConfig):
    """Create a psycopg2 connection to RisingWave."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "RisingWave support requires the 'psycopg2' package. "
            "Install with: pip install psycopg2-binary"
        ) from None

    rw = config.risingwave
    return psycopg2.connect(
        host=rw.host,
        port=rw.port,
        user=rw.user,
        password=rw.password,
        database=rw.database,
    )


def _qualified(schema: str, name: str):
    """Build a schema-qualified identifier: schema.name"""
    from psycopg2 import sql
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name))


def _ensure_risingwave_objects(conn, rw: RisingWaveConfig) -> None:
    """Create MV, subscription, and progress table if they do not exist.

    Idempotent — safe to call on every run. Retention is set at subscription
    creation time. If the subscription already exists with a different
    retention, it is left unchanged (ALTER is not supported by RisingWave).
    """
    from psycopg2 import sql

    q_source = _qualified(rw.schema_name, rw.source_table)
    q_mv = _qualified(rw.schema_name, rw.mv_name)
    q_sub = _qualified(rw.schema_name, rw.subscription_name)
    q_progress = _qualified(rw.schema_name, rw.progress_table)

    with conn.cursor() as cur:
        # Materialized view: flatten JSONB into typed columns
        cur.execute(sql.SQL(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {} AS "
            "SELECT "
            "  data ->> 'id'           AS id,"
            "  data ->> 'type'         AS type,"
            "  data ->> 'source'       AS source,"
            "  data ->> 'namespace'    AS namespace,"
            "  data ->> 'name'         AS name,"
            "  data ->> 'tabular_id'   AS tabular_id,"
            "  data ->> 'warehouse_id' AS warehouse_id,"
            "  data ->> 'trace_id'     AS trace_id,"
            "  (data -> 'data')::JSONB AS data "
            "FROM {}"
        ).format(q_mv, q_source))

        # Subscription on the MV
        cur.execute(sql.SQL(
            "CREATE SUBSCRIPTION IF NOT EXISTS {} "
            "FROM {} WITH (retention = %s)"
        ).format(q_sub, q_mv), (rw.retention,))

        # Progress table: single-row cursor tracking
        cur.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ("
            "  progress BIGINT PRIMARY KEY"
            ") ON CONFLICT OVERWRITE"
        ).format(q_progress))

    logger.debug(
        "Ensured RisingWave objects: mv=%s sub=%s progress=%s",
        rw.mv_name, rw.subscription_name, rw.progress_table,
    )


def _read_cursor(conn, rw: RisingWaveConfig) -> int | None:
    """Read the current cursor progress."""
    from psycopg2 import sql

    q_progress = _qualified(rw.schema_name, rw.progress_table)
    with conn.cursor() as cur:
        try:
            cur.execute(sql.SQL("SELECT progress FROM {}").format(q_progress))
            result = cur.fetchone()
        except Exception:
            return None
    if result and result[0] is not None:
        return int(result[0])
    return None


def save_cursor(config: EventsConfig, timestamp: int) -> None:
    """Save cursor progress to RisingWave.

    Call this ONLY after a successful sync. Uses INSERT into a table with
    ON CONFLICT OVERWRITE — the single row is replaced unconditionally.
    """
    from psycopg2 import sql

    conn = _connect(config)
    try:
        rw = config.risingwave
        q_progress = _qualified(rw.schema_name, rw.progress_table)
        _ensure_risingwave_objects(conn, rw)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO {} (progress) VALUES (%s)").format(q_progress),
                (timestamp,),
            )
            cur.execute("FLUSH")
        conn.commit()
        logger.info("Cursor advanced to rw_timestamp=%d", timestamp)
    finally:
        conn.close()


def consume_events(config: EventsConfig) -> tuple[list[dict], int | None]:
    """Consume pending events from a RisingWave subscription.

    Connects via psycopg2, ensures all RisingWave objects exist (MV,
    subscription, progress table), reads the cursor, declares a
    subscription cursor SINCE that point, and fetches pending events.

    Returns (rows, latest_rw_timestamp). Does NOT advance the cursor —
    the caller must call save_cursor() after successful sync.
    """
    from psycopg2 import sql

    conn = _connect(config)
    rw = config.risingwave
    q_sub = _qualified(rw.schema_name, rw.subscription_name)

    try:
        _ensure_risingwave_objects(conn, rw)
        last_ts = _read_cursor(conn, rw)

        cur = conn.cursor()

        if last_ts is not None:
            cur.execute(sql.SQL(
                "DECLARE ics_cursor SUBSCRIPTION CURSOR FOR {} SINCE %s"
            ).format(q_sub), (last_ts,))
            logger.info("Resuming from rw_timestamp=%d", last_ts)
        else:
            cur.execute(sql.SQL(
                "DECLARE ics_cursor SUBSCRIPTION CURSOR FOR {}"
            ).format(q_sub))
            logger.info("No cursor found, starting from current position")

        all_rows: list[dict] = []
        latest_timestamp: int | None = None
        remaining = config.max_events

        while remaining > 0:
            cur.execute("FETCH NEXT FROM ics_cursor")
            row = cur.fetchone()
            if row is None:
                break

            row_dict = _tuple_to_dict(row)
            all_rows.append(row_dict)

            rw_ts = row_dict.get("rw_timestamp")
            if rw_ts is not None and (
                latest_timestamp is None or rw_ts > latest_timestamp
            ):
                latest_timestamp = rw_ts

            remaining -= 1

        logger.info(
            "Consumed %d events from RisingWave subscription '%s'",
            len(all_rows),
            rw.subscription_name,
        )

        return all_rows, latest_timestamp
    finally:
        conn.close()


# Column order from the lakekeeper_events MV + subscription metadata.
# The subscription appends op (INT16) and rw_timestamp (BIGINT) columns
# after the MV columns.
_MV_COLUMNS = (
    "id", "type", "source", "namespace", "name",
    "tabular_id", "warehouse_id", "trace_id", "data",
    "op", "rw_timestamp",
)


def _tuple_to_dict(row: tuple) -> dict:
    """Convert a raw tuple from FETCH into a dict using MV column order."""
    return dict(zip(_MV_COLUMNS, row))
