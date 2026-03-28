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
  namespaces:
    - my_database
    - another_db
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
- **Both, same metadata** → skip (up to date)
- **Both, different metadata** → `drop_table` + `register_table` (update)
- **Dest only** → `drop_table` if `drop_orphan_tables: true`, else skip

```bash
iceberg-catalog-sync --config sync.yaml --mode full
```

### Event-Driven Partial Sync (via RisingWave)

Consumes CloudEvents published by [Lakekeeper](https://github.com/lakekeeper/lakekeeper) to a RisingWave subscription. Instead of scanning all tables, only the specific tables that changed are synced. This dramatically reduces API calls to both catalogs.

```bash
iceberg-catalog-sync --config sync.yaml --mode events
```

When `events.enabled: true` in config, `--mode events` is the default.

**Benefits over full sync:**

- No `list_tables` calls (the most expensive catalog API call)
- Only loads and compares the specific tables that changed
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
                  └─ partial sync: only affected tables
```

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
  namespaces:                    # Namespaces to sync (required)
    - my_database
    - another_db
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
    subscription_name: lakekeeper_events_subscription
    cursor_path: .iceberg-catalog-sync-cursor.json  # Offset tracking file
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

**Step 1: Create the webhook source table** (single JSONB column):

```sql
CREATE TABLE lakekeeper_events_raw (
    data JSONB
) WITH (
    connector = 'webhook'
);
```

**Step 2: Create a materialized view** to flatten JSONB into typed columns:

```sql
CREATE MATERIALIZED VIEW lakekeeper_events AS
SELECT
    data ->> 'id'           AS id,
    data ->> 'type'         AS type,
    data ->> 'source'       AS source,
    data ->> 'namespace'    AS namespace,
    data ->> 'name'         AS name,
    data ->> 'tabular_id'   AS tabular_id,
    data ->> 'warehouse_id' AS warehouse_id,
    data ->> 'trace_id'     AS trace_id,
    (data -> 'data')::JSONB AS data
FROM lakekeeper_events_raw;
```

The MV is incrementally maintained — RisingWave only processes new rows as they arrive.

**Step 3: Create the subscription:**

```sql
CREATE SUBSCRIPTION lakekeeper_events_subscription
    FROM lakekeeper_events
    WITH (retention = '7 days');
```

The `retention` parameter controls how long consumed events are kept. Adjust based on your consumer's recovery window.

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

The sync tool tracks its position in the RisingWave subscription using `rw_timestamp` values, stored in a local JSON file (configurable via `cursor_path`). This ensures exactly-once processing across cron runs.

On first run (or if the cursor file is missing), consumption starts from `begin()` (oldest available data). Subsequent runs resume from the last processed timestamp.

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
    sync=SyncBehaviorConfig(namespaces=["my_db"]),
)
result = sync_catalogs(config)

# ── Event-driven partial sync ──
from iceberg_catalog_sync.events import build_changeset_from_rows, consume_events

rows, _ = consume_events(config.events)
changeset = build_changeset_from_rows(rows, managed_namespaces={"my_db"})
result = sync_from_changeset(config, changeset)
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
