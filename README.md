# Iceberg Catalog Sync

Sync Iceberg table metadata between two Iceberg REST catalogs. No data movement — only catalog pointers (`metadata.json` locations) are synced.

**Architecture:** Source catalog (single writer, source of truth) → Destination catalog (read-only mirror, same metadata files on shared storage)

## Features

- **Full sync** (pull-based) — scans all managed namespaces, diffs source vs destination, applies changes
- **Event-driven partial sync** — consumes CloudEvents from RisingWave to sync only the tables that changed, avoiding expensive full scans
- **Idempotent and stateless** — safe to restart at any point, no persistent state between runs
- **Rate-limit aware** — configurable retry with exponential backoff for rate-limited destination catalogs
- **Dry-run mode** — preview all changes without mutating the destination
- **Observability** — structured logging (text/JSON), OpenTelemetry metrics and tracing
- **Library + CLI** — use from Airflow/Python directly, or invoke via cron/K8s CronJob

## Installation

```bash
# Core install
pip install iceberg-catalog-sync

# With RisingWave event-driven sync support
pip install 'iceberg-catalog-sync[risingwave]'
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv sync                                    # core only
uv sync --extra risingwave                 # with RisingWave support
```

## Quick Start

### 1. Create a config file

```yaml
# sync.yaml
catalogs:
  source:
    name: lakekeeper
    uri: https://lakekeeper.example.com/catalog
    warehouse: my-warehouse
    properties:
      token: ${LAKEKEEPER_TOKEN}

  destination:
    name: other-catalog
    uri: https://other-catalog.example.com/iceberg/v1
    warehouse: projects/my-project/locations/us/catalogs/my-catalog
    properties:
      token: ${DEST_TOKEN}

sync:
  exclude_namespaces:           # Optional: skip these namespaces
    - system
  dry_run: false
  drop_orphan_tables: false
  sync_namespace_properties: true

retry:
  max_attempts: 5
  base_delay_seconds: 1.0
  max_delay_seconds: 60.0
```

### 2. Run a full sync

```bash
iceberg-catalog-sync --config sync.yaml
```

### 3. Or use from Python

```python
from iceberg_catalog_sync import sync_catalogs, load_config

config = load_config("sync.yaml")
result = sync_catalogs(config)
assert result.success
print(result.summary)
```

## Sync Modes

### Full Sync (pull-based)

The default mode. Scans all tables in each managed namespace, compares metadata locations between source and destination, and applies the diff:

- **Source only** → `register_table` in destination
- **Both, same metadata** → skip (up to date, no API calls)
- **Both, different metadata** → update (drop + register)
- **Dest only** → `drop_table` if `drop_orphan_tables: true`, else skip

```bash
iceberg-catalog-sync --config sync.yaml --mode full
```

**Note on table updates:** The Iceberg REST catalog doesn't provide an atomic "update metadata location" API for external table registration. Updates use drop+register which creates a brief window (milliseconds) where the table doesn't exist. The operation is idempotent — if it fails midway, re-running completes it. Data files are never at risk — only the catalog metadata pointer changes.

**Optimization:** Full sync compares `metadata_location` before updating, so unchanged tables are skipped (0 API calls to destination). This is efficient when <10% of tables change between syncs.

### Event-Driven Partial Sync (via RisingWave)

Consumes CloudEvents published by [Lakekeeper](https://github.com/lakekeeper/lakekeeper) to a RisingWave subscription. Instead of scanning all tables, only the specific tables that changed are synced. This dramatically reduces API calls to both catalogs.

```bash
iceberg-catalog-sync --config sync.yaml --mode events
```

When `events.enabled: true` in config, `--mode events` is the default.

**Benefits over full sync:**

- No `list_tables` calls (the most expensive catalog API call)
- No `load_table` calls to destination (always overrides without comparison)
- Only loads tables from source that changed
- Fewer API calls to the destination catalog (critical for rate-limited SaaS destinations)
- Lower operational cost

**How it works:**

```
Lakekeeper catalog mutation
  → CloudEventsPublisher
    → RisingWaveBackend (HTTP POST, JSON)
      → RisingWave webhook listener (:4560)
        → lakekeeper_events_raw table (single JSONB column)
          → lakekeeper_events materialized view (flat typed columns)
            → lakekeeper_events_subscription
              → iceberg-catalog-sync (risingwave-py, cursor-based)
                  ├─ filter to managed namespaces
                  ├─ deduplicate into ChangeSet
                  └─ partial sync: always override (drop + register)
```

**Note:** Event-driven sync always overrides destination tables without comparison. Events indicate a change occurred, so we apply the change directly (drop + register). This is more efficient than comparing when the change rate is high.

## Configuration Reference

All behavior is driven by a YAML config file. Environment variables are supported with `${VAR}` and `${VAR:-default}` syntax.

```yaml
# ─── Catalogs ───
catalogs:
  source:
    name: lakekeeper                                    # Catalog identifier
    uri: https://lakekeeper.example.com/catalog         # Iceberg REST catalog URI
    warehouse: my-warehouse                             # Warehouse identifier
    properties:                                         # Additional catalog properties
      token: ${LAKEKEEPER_TOKEN}

  destination:
    name: other-catalog
    uri: https://other.example.com/iceberg/v1
    warehouse: my-dest-warehouse
    properties:
      token: ${DEST_TOKEN}

# ─── Sync behavior ───
sync:
  exclude_namespaces: [system]   # Skip these namespaces (default: none)
  dry_run: false                 # Preview changes without mutating destination
  drop_orphan_tables: false      # Drop tables in dest that don't exist in source
  sync_namespace_properties: true  # Sync namespace properties (additive only)

# ─── Retry (for rate-limited destinations) ───
retry:
  max_attempts: 5                # Max retry attempts per operation
  base_delay_seconds: 1.0        # Initial backoff delay
  max_delay_seconds: 60.0        # Maximum backoff delay

# ─── Logging ───
log:
  level: INFO                    # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: text                   # text or json

# ─── Event-driven sync (via RisingWave) ───
events:
  enabled: false
  risingwave:
    host: localhost
    port: 4566
    user: root
    password: ${RW_PASSWORD:-root}
    database: dev
    schema_name: public                        # Schema for all managed objects
    source_table: lakekeeper_events_raw        # Webhook source table (must already exist)
    # Derived objects (auto-created in same database/schema):
    #   MV:        __ics_lakekeeper_events_raw_mv
    #   Sub:       __ics_lakekeeper_events_raw_sub
    #   Progress:  __ics_lakekeeper_events_raw_progress
  sync_interval_seconds: 60      # Batch window — events are collected and synced as one batch
  max_events: 1000               # Max events to process per run

# ─── OpenTelemetry Metrics ───
metrics:
  enabled: false
  exporter: otlp                 # otlp or console
  otlp_endpoint: http://localhost:4317

# ─── OpenTelemetry Tracing ───
tracing:
  enabled: false
  exporter: otlp                 # otlp or console
  otlp_endpoint: http://localhost:4317
  service_name: iceberg-catalog-sync
```

## Event-Driven Sync Setup with RisingWave

### Prerequisites

1. **RisingWave** v2.2+ with webhook listener enabled
2. **Lakekeeper** built with the `risingwave` feature flag, configured to publish CloudEvents to RisingWave's webhook endpoint

### RisingWave Setup

Lakekeeper publishes CloudEvents to RisingWave's built-in [webhook source](https://docs.risingwave.com/integrations/sources/webhook), which is embedded in the RisingWave frontend node — no extra services to deploy. The webhook uses RisingWave's FastInsert RPC path (bypasses SQL parser), and `wait_for_persistence=true` by default means HTTP 200 = data durably committed.

The only prerequisite is the webhook source table. **All other objects are auto-created by iceberg-catalog-sync on first run.**

**Create the webhook source table** (single JSONB column):

```sql
CREATE TABLE lakekeeper_events_raw (
    data JSONB
) WITH (
    connector = 'webhook'
);
```

That's it. On first run, iceberg-catalog-sync creates these objects in the same database and schema:

| Object | Name | Purpose |
|--------|------|---------|
| Materialized View | `__ics_lakekeeper_events_raw_mv` | Flattens JSONB into typed columns |
| Subscription | `__ics_lakekeeper_events_raw_sub` | Change feed from the MV |
| Progress Table | `__ics_lakekeeper_events_raw_progress` | Cursor tracking (single-row, `ON CONFLICT OVERWRITE`) |

All names are derived from `source_table`, prefixed with `__ics_` to signal internal/private objects. All `CREATE` statements use `IF NOT EXISTS` — idempotent across runs.

### Initial Run Behavior

On the first run (when the subscription doesn't exist yet):
1. `setup_risingwave()` detects subscription doesn't exist → `is_initial_run = true`
2. Drops progress table if exists (clean slate)
3. Creates all RisingWave objects (MV, subscription, progress table)
4. **Runs a full sync** to establish the baseline in the destination catalog
5. `consume_events()` — cursor starts from `SINCE begin()` (oldest data in retention window)

This ensures the destination catalog is fully synchronized before incremental event-driven sync begins. The full sync establishes the baseline, and the subscription then captures ongoing changes.

### Retention Configuration

The `retention` setting (default: `"1 day"`) controls how long RisingWave retains changelog history for the subscription. This is set **only at subscription creation time**.

**Important:** RisingWave does not support `ALTER SUBSCRIPTION`. To change retention:
1. Manually drop the subscription: `DROP SUBSCRIPTION __ics_lakekeeper_events_raw_sub;`
2. Update `events.risingwave.retention` in your config file
3. Run a **full sync** first (the new subscription has no history before its creation point)
4. Resume event-driven mode

```yaml
events:
  risingwave:
    retention: "7 days"  # Set at creation time only
```

### Lakekeeper Setup

Configure Lakekeeper to publish to the webhook endpoint:

```bash
export LAKEKEEPER__RISINGWAVE_WEBHOOK_URL="http://risingwave:4560/webhook/dev/public/lakekeeper_events_raw"
```

The URL format is `http://<host>:<port>/webhook/<database>/<schema>/<table>`. The webhook listener runs on port `4560` of each RisingWave frontend node.

Each CloudEvent is flattened into a JSON object with these fields (used by the sync tool):

| Field | Description | Example |
|-------|-------------|---------|
| `type` | Event type | `createTable`, `updateTable`, `dropTable` |
| `namespace` | Dot-separated namespace path | `my_database` |
| `name` | Table/view name | `my_table` |
| `tabular_id` | UUID of the table/view | `Table/550e8400-...` |
| `warehouse_id` | UUID of the warehouse | `6ba7b810-...` |

### Cursor Tracking

The sync tool tracks its position using `rw_timestamp` values (Unix milliseconds), stored in the progress table in RisingWave itself. This keeps iceberg-catalog-sync stateless — no local files needed.

The cursor is advanced **only after a successful sync**. If sync fails, the cursor stays where it was and events will be re-processed on the next run (at-least-once semantics). Since the sync is idempotent, reprocessing is safe.

**Cursor syntax:** The subscription cursor uses RisingWave's `SINCE` clause:
- `SINCE <unix_ms>` — resume from a specific timestamp (used when restarting)
- `SINCE begin()` — start from oldest available data (used on initial run or no saved cursor)
- `SINCE now()` — start from declaration time (not used by this tool)
- `FULL` — read existing snapshot first, then incremental (not used by this tool)

The progress table follows the pattern from the [RisingWave subscription docs](https://docs.risingwave.com/serve/subscription):

```sql
CREATE TABLE IF NOT EXISTS __ics_lakekeeper_events_raw_progress (
    progress BIGINT PRIMARY KEY
) ON CONFLICT OVERWRITE;
```

## Library API

For use in Airflow, custom scripts, or other Python applications:

```python
from iceberg_catalog_sync import (
    sync_catalogs,
    sync_from_changeset,
    load_config,
    AppConfig,
    ChangeSet,
    SyncResult,
    setup_risingwave,
    consume_events,
    build_changeset_from_rows,
    save_cursor,
)

# ── Full sync from config file ──
config = load_config("sync.yaml")
result = sync_catalogs(config)
print(result.summary)  # {'register': 3, 'update': 1, 'skip': 10, 'errors': 0}

# ── Full sync with programmatic config ──
from iceberg_catalog_sync.config import (
    CatalogConfig,
    CatalogsConfig,
    SyncBehaviorConfig,
)

config = AppConfig(
    catalogs=CatalogsConfig(
        source=CatalogConfig(name="src", uri="http://...", warehouse="wh"),
        destination=CatalogConfig(name="dst", uri="http://...", warehouse="wh"),
    ),
    sync=SyncBehaviorConfig(),  # syncs all source namespaces by default
)
result = sync_catalogs(config)

# ── Event-driven partial sync ──
# Flow: setup → (optional full sync) → consume → sync → save_cursor

is_initial = setup_risingwave(config.events)
if is_initial:
    # First run: establish baseline with full sync
    full_result = sync_catalogs(config)
    if not full_result.success:
        raise RuntimeError("Initial full sync failed")

# Consume events (on initial run, gets events after full sync completed)
rows, latest_ts = consume_events(config.events)
if rows:
    changeset = build_changeset_from_rows(rows)
    result = sync_from_changeset(config, changeset)
    if result.success and latest_ts is not None:
        save_cursor(config.events, latest_ts)  # advance cursor only on success
```

## CLI Reference

```
iceberg-catalog-sync --config <path> [--mode full|events]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--config` | Yes | Path to YAML config file |
| `--mode` | No | `full` (scan all) or `events` (partial sync from RisingWave). Defaults to `events` if `events.enabled`, otherwise `full` |

Exit codes: `0` on success, `1` on errors.

## Deployment

### Cron / K8s CronJob

```bash
# Full sync every 15 minutes
*/15 * * * * iceberg-catalog-sync --config /etc/sync.yaml --mode full

# Event-driven sync every minute (lightweight — only processes changes)
* * * * * iceberg-catalog-sync --config /etc/sync.yaml --mode events
```

### Airflow

```python
from airflow.decorators import task

@task
def sync_catalogs_task():
    from iceberg_catalog_sync import sync_catalogs, load_config
    config = load_config("/etc/sync.yaml")
    result = sync_catalogs(config)
    if not result.success:
        raise RuntimeError(f"Sync failed: {result.errors}")
```

## Observability

### Metrics (OpenTelemetry)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `sync.tables.registered` | Counter | `namespace` | Tables newly registered in dest |
| `sync.tables.updated` | Counter | `namespace` | Tables with updated metadata location |
| `sync.tables.dropped` | Counter | `namespace` | Orphan tables dropped from dest |
| `sync.tables.up_to_date` | Counter | `namespace` | Tables already in sync |
| `sync.tables.errors` | Counter | `namespace` | Per-table sync failures |
| `sync.namespaces.created` | Counter | — | Namespaces created in dest |
| `sync.duration_seconds` | Histogram | — | Total sync duration |
| `sync.retries` | Counter | `operation` | Retry attempts due to rate limits |

### Traces

- **Root span:** `sync_catalogs` or `sync_from_changeset`
  - **Child span:** `sync_namespace({ns})`
    - **Child span:** `register_table`, `update_table`, `drop_table`

### Logging

- `DEBUG` — API calls, metadata locations, skipped events
- `INFO` — Actions taken (registered, updated, created namespace)
- `WARNING` — Retries, orphan tables skipped
- `ERROR` — Per-table failures (non-fatal)

## Development

```bash
# Install dev dependencies
uv sync

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=iceberg_catalog_sync
```
