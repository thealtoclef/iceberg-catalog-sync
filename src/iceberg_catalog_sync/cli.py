"""Click CLI — single entrypoint with --config and --mode."""

from __future__ import annotations

import sys

import click

from iceberg_catalog_sync.config import load_config
from iceberg_catalog_sync.sync import sync_catalogs, sync_from_changeset


@click.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to YAML configuration file.",
)
@click.option(
    "--mode",
    type=click.Choice(["full", "events"]),
    default=None,
    help=(
        "Sync mode. 'full' scans all managed namespaces (pull-based). "
        "'events' consumes CloudEvents from RisingWave for partial sync. "
        "Defaults to 'events' if events.enabled is true, otherwise 'full'."
    ),
)
def main(config_path: str, mode: str | None) -> None:
    """Iceberg Catalog Sync — sync table metadata between two Iceberg REST catalogs."""
    try:
        config = load_config(config_path)
    except Exception as exc:
        click.echo(f"Error loading config: {exc}", err=True)
        sys.exit(1)

    # Resolve mode from config if not explicitly set
    if mode is None:
        mode = "events" if config.events.enabled else "full"

    if mode == "events":
        if not config.events.enabled:
            click.echo(
                "Error: --mode=events requires events.enabled=true in config",
                err=True,
            )
            sys.exit(1)

        from iceberg_catalog_sync.events import (
            build_changeset_from_rows,
            consume_events,
            save_cursor,
            setup_risingwave,
        )

        is_initial_run = setup_risingwave(config.events)

        # On initial run: run full sync FIRST to establish baseline
        if is_initial_run:
            click.echo("Initial run: running full sync first to establish baseline...")
            full_result = sync_catalogs(config)
            click.echo(f"Full sync complete: {full_result.summary}")

            if not full_result.success:
                click.echo(
                    f"Full sync had {len(full_result.errors)} error(s). "
                    "Event mode aborted — fix errors and re-run.",
                    err=True,
                )
                sys.exit(1)

        # Now consume events (on initial run, this gets events after full sync completed)
        rows, latest_ts = consume_events(config.events)

        if not rows:
            click.echo("No pending events, nothing to sync.")
            return

        exclude = set(config.sync.exclude_namespaces)
        changeset = build_changeset_from_rows(
            rows, exclude_namespaces=exclude or None
        )
        if changeset.is_empty:
            click.echo("No relevant changes in events, nothing to sync.")
            # Still advance cursor — these events were consumed but irrelevant
            if latest_ts is not None:
                save_cursor(config.events, latest_ts)
            return

        result = sync_from_changeset(config, changeset)

        # Advance cursor ONLY after successful sync
        if result.success and latest_ts is not None:
            save_cursor(config.events, latest_ts)
        elif not result.success and latest_ts is not None:
            click.echo(
                "Sync had errors — cursor NOT advanced. "
                "Events will be re-processed on next run.",
                err=True,
            )
    else:
        result = sync_catalogs(config)

    click.echo(f"\nSync {'(DRY RUN) ' if result.dry_run else ''}complete ({mode} mode):")
    for key, count in result.summary.items():
        click.echo(f"  {key}: {count}")

    if not result.success:
        click.echo(f"\n{len(result.errors)} error(s) occurred:", err=True)
        for error in result.errors:
            table_info = f".{error.table}" if error.table else ""
            click.echo(f"  {error.namespace}{table_info}: {error.error}", err=True)
        sys.exit(1)
