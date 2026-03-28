"""Tests for core sync algorithm — full sync and partial sync from changeset."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)

from iceberg_catalog_sync.config import AppConfig
from iceberg_catalog_sync.events import ChangeSet
from iceberg_catalog_sync.reporting import ActionType
from iceberg_catalog_sync.sync import sync_catalogs, sync_from_changeset
from tests.conftest import make_mock_table


@pytest.fixture
def mock_catalogs():
    """Fixture that patches load_catalog and returns (source_mock, dest_mock)."""
    with patch("iceberg_catalog_sync.sync.load_catalog") as mock_load:
        source = MagicMock()
        dest = MagicMock()
        mock_load.side_effect = lambda name, **kwargs: (
            source if name == "source" else dest
        )
        yield source, dest


# ── Full sync tests ──


class TestSyncNewTable:
    def test_register_new_table(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        source.list_tables.return_value = [("test_ns", "table_a")]
        dest.list_tables.return_value = []
        dest.load_namespace_properties.return_value = {"key": "val"}
        source.load_table.return_value = make_mock_table("s3://bucket/table_a/metadata/v1.metadata.json")
        source.load_namespace_properties.return_value = {}

        result = sync_catalogs(base_config)

        dest.register_table.assert_called_once_with(
            ("test_ns", "table_a"),
            "s3://bucket/table_a/metadata/v1.metadata.json",
        )
        assert result.success
        register_actions = [a for a in result.actions if a.action == ActionType.REGISTER]
        assert len(register_actions) == 1
        assert register_actions[0].table == "table_a"


class TestSyncUpToDate:
    def test_skip_up_to_date_table(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        loc = "s3://bucket/t/metadata/v1.metadata.json"
        source.list_tables.return_value = [("test_ns", "table_a")]
        dest.list_tables.return_value = [("test_ns", "table_a")]
        source.load_table.return_value = make_mock_table(loc)
        dest.load_table.return_value = make_mock_table(loc)
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        result = sync_catalogs(base_config)

        dest.register_table.assert_not_called()
        dest.drop_table.assert_not_called()
        skip_actions = [a for a in result.actions if a.action == ActionType.SKIP]
        assert len(skip_actions) == 1


class TestSyncUpdateTable:
    def test_update_changed_metadata(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        source.list_tables.return_value = [("test_ns", "table_a")]
        dest.list_tables.return_value = [("test_ns", "table_a")]
        source.load_table.return_value = make_mock_table("s3://bucket/t/v2.metadata.json")
        dest.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        result = sync_catalogs(base_config)

        dest.drop_table.assert_called_once_with(("test_ns", "table_a"))
        dest.register_table.assert_called_once_with(
            ("test_ns", "table_a"),
            "s3://bucket/t/v2.metadata.json",
        )
        update_actions = [a for a in result.actions if a.action == ActionType.UPDATE]
        assert len(update_actions) == 1


class TestSyncDropOrphan:
    def test_drop_orphan_when_enabled(self, base_config, mock_catalogs):
        base_config.sync.drop_orphan_tables = True
        source, dest = mock_catalogs
        source.list_tables.return_value = []
        dest.list_tables.return_value = [("test_ns", "orphan_table")]
        dest.load_table.return_value = make_mock_table("s3://bucket/orphan/v1.metadata.json")
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        result = sync_catalogs(base_config)

        dest.drop_table.assert_called_once_with(("test_ns", "orphan_table"))
        drop_actions = [a for a in result.actions if a.action == ActionType.DROP]
        assert len(drop_actions) == 1

    def test_skip_orphan_when_disabled(self, base_config, mock_catalogs):
        base_config.sync.drop_orphan_tables = False
        source, dest = mock_catalogs
        source.list_tables.return_value = []
        dest.list_tables.return_value = [("test_ns", "orphan_table")]
        dest.load_table.return_value = make_mock_table("s3://bucket/orphan/v1.metadata.json")
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        result = sync_catalogs(base_config)

        dest.drop_table.assert_not_called()
        assert not any(a.action == ActionType.DROP for a in result.actions)


class TestSyncNamespace:
    def test_create_missing_namespace(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        dest.load_namespace_properties.side_effect = NoSuchNamespaceError("test_ns")
        source.load_namespace_properties.return_value = {"location": "s3://bucket/ns"}
        source.list_tables.return_value = []
        dest.list_tables.return_value = []

        result = sync_catalogs(base_config)

        dest.create_namespace.assert_called_once_with(
            ("test_ns",), {"location": "s3://bucket/ns"}
        )
        ns_actions = [a for a in result.actions if a.action == ActionType.CREATE_NAMESPACE]
        assert len(ns_actions) == 1

    def test_sync_namespace_properties_additive(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {"existing": "val"}
        source.load_namespace_properties.return_value = {"existing": "val", "new_key": "new_val"}
        source.list_tables.return_value = []
        dest.list_tables.return_value = []

        result = sync_catalogs(base_config)

        dest.update_namespace_properties.assert_called_once_with(
            ("test_ns",), removals=set(), updates={"new_key": "new_val"}
        )
        prop_actions = [a for a in result.actions if a.action == ActionType.UPDATE_NAMESPACE_PROPERTIES]
        assert len(prop_actions) == 1


class TestDryRun:
    def test_dry_run_no_mutations(self, base_config, mock_catalogs):
        base_config.sync.dry_run = True
        source, dest = mock_catalogs
        dest.load_namespace_properties.side_effect = NoSuchNamespaceError("test_ns")
        source.load_namespace_properties.return_value = {}
        source.list_tables.return_value = [("test_ns", "table_a")]
        dest.list_tables.return_value = []
        source.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")

        result = sync_catalogs(base_config)

        dest.create_namespace.assert_not_called()
        dest.register_table.assert_not_called()
        dest.drop_table.assert_not_called()
        assert result.dry_run is True
        assert all(a.dry_run for a in result.actions)


class TestErrorIsolation:
    def test_table_error_does_not_stop_sync(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}
        source.list_tables.return_value = [
            ("test_ns", "good_table"),
            ("test_ns", "bad_table"),
        ]
        dest.list_tables.return_value = []

        good_table = make_mock_table("s3://bucket/good/v1.metadata.json")
        source.load_table.side_effect = lambda tid: (
            good_table if tid[-1] == "good_table" else (_ for _ in ()).throw(Exception("load failed"))
        )

        result = sync_catalogs(base_config)

        # good_table should still be registered
        dest.register_table.assert_called_once()
        assert len(result.errors) == 1
        assert result.errors[0].table == "bad_table"


# ── Partial sync (changeset) tests ──


class TestSyncFromChangeset:
    def test_register_new_table_from_changeset(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        source.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")
        dest.load_table.side_effect = NoSuchTableError("not found")

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "new_table")

        result = sync_from_changeset(base_config, changeset)

        dest.register_table.assert_called_once_with(
            ("test_ns", "new_table"),
            "s3://bucket/t/v1.metadata.json",
        )
        assert result.success

    def test_update_table_from_changeset(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        source.load_table.return_value = make_mock_table("s3://bucket/t/v2.metadata.json")
        dest.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "updated_table")

        result = sync_from_changeset(base_config, changeset)

        dest.drop_table.assert_called_once()
        dest.register_table.assert_called_once()
        update_actions = [a for a in result.actions if a.action == ActionType.UPDATE]
        assert len(update_actions) == 1

    def test_skip_up_to_date_from_changeset(self, base_config, mock_catalogs):
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        loc = "s3://bucket/t/v1.metadata.json"
        source.load_table.return_value = make_mock_table(loc)
        dest.load_table.return_value = make_mock_table(loc)

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "same_table")

        result = sync_from_changeset(base_config, changeset)

        dest.register_table.assert_not_called()
        dest.drop_table.assert_not_called()
        skip_actions = [a for a in result.actions if a.action == ActionType.SKIP]
        assert len(skip_actions) == 1

    def test_drop_table_from_changeset(self, base_config, mock_catalogs):
        base_config.sync.drop_orphan_tables = True
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        source.load_table.side_effect = NoSuchTableError("gone")
        dest.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "dropped_table")

        result = sync_from_changeset(base_config, changeset)

        dest.drop_table.assert_called_once()
        drop_actions = [a for a in result.actions if a.action == ActionType.DROP]
        assert len(drop_actions) == 1

    def test_no_list_tables_called(self, base_config, mock_catalogs):
        """Partial sync should NOT call list_tables — that's the whole point."""
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        source.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")
        dest.load_table.side_effect = NoSuchTableError("not found")

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "table_a")

        sync_from_changeset(base_config, changeset)

        source.list_tables.assert_not_called()
        dest.list_tables.assert_not_called()

    def test_filters_to_managed_namespaces(self, base_config, mock_catalogs):
        """Changeset may include namespaces not in config — those are skipped."""
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        source.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")
        dest.load_table.side_effect = NoSuchTableError("not found")

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "table_a")  # managed
        changeset.add_table_change("other_ns", "table_b")  # not managed

        result = sync_from_changeset(base_config, changeset)

        # Only test_ns table should be processed
        dest.register_table.assert_called_once()
        call_args = dest.register_table.call_args[0]
        assert call_args[0] == ("test_ns", "table_a")

    def test_dry_run_from_changeset(self, base_config, mock_catalogs):
        base_config.sync.dry_run = True
        source, dest = mock_catalogs
        dest.load_namespace_properties.return_value = {}
        source.load_namespace_properties.return_value = {}

        source.load_table.return_value = make_mock_table("s3://bucket/t/v1.metadata.json")
        dest.load_table.side_effect = NoSuchTableError("not found")

        changeset = ChangeSet()
        changeset.add_table_change("test_ns", "table_a")

        result = sync_from_changeset(base_config, changeset)

        dest.register_table.assert_not_called()
        assert result.dry_run is True
        assert all(a.dry_run for a in result.actions)
