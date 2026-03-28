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
        )

        rows, _ = consume_events(config.events)
        if not rows:
            click.echo("No pending events, nothing to sync.")
            return

        managed = set(config.sync.namespaces)
        changeset = build_changeset_from_rows(rows, managed)
        if changeset.is_empty:
            click.echo("No relevant changes in events, nothing to sync.")
            return

        result = sync_from_changeset(config, changeset)
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
