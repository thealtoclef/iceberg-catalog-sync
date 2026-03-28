"""Tests for event consumption and ChangeSet building."""

from __future__ import annotations

import json

import pytest

from iceberg_catalog_sync.events import (
    ChangeSet,
    build_changeset_from_rows,
    _load_cursor_timestamp,
    _save_cursor_timestamp,
)


class TestChangeSet:
    def test_empty_changeset(self):
        cs = ChangeSet()
        assert cs.is_empty
        assert cs.affected_namespaces == set()
        assert cs.affected_tables("ns") == set()

    def test_add_table_change(self):
        cs = ChangeSet()
        cs.add_table_change("ns1", "table_a")
        cs.add_table_change("ns1", "table_b")
        cs.add_table_change("ns2", "table_c")

        assert not cs.is_empty
        assert cs.affected_namespaces == {"ns1", "ns2"}
        assert cs.affected_tables("ns1") == {"table_a", "table_b"}
        assert cs.affected_tables("ns2") == {"table_c"}

    def test_add_namespace_change(self):
        cs = ChangeSet()
        cs.add_namespace_change("ns1")

        assert cs.has_namespace_changes("ns1")
        assert not cs.has_namespace_changes("ns2")
        assert cs.affected_namespaces == {"ns1"}

    def test_dedup_table_changes(self):
        cs = ChangeSet()
        cs.add_table_change("ns1", "table_a")
        cs.add_table_change("ns1", "table_a")
        cs.add_table_change("ns1", "table_a")

        assert cs.affected_tables("ns1") == {"table_a"}

    def test_repr(self):
        cs = ChangeSet()
        cs.add_table_change("ns1", "t1")
        cs.add_table_change("ns1", "t2")
        cs.raw_event_count = 5
        assert "namespaces=1" in repr(cs)
        assert "tables=2" in repr(cs)
        assert "raw_events=5" in repr(cs)


class TestBuildChangesetFromRows:
    def _make_row(self, event_type, namespace, name="", op="Insert"):
        return {
            "op": op,
            "rw_timestamp": 1234567890,
            "type": event_type,
            "namespace": namespace,
            "name": name,
        }

    def test_table_events(self):
        managed = {"ns1", "ns2"}
        rows = [
            self._make_row("createTable", "ns1", "table_a"),
            self._make_row("updateTable", "ns1", "table_b"),
            self._make_row("dropTable", "ns2", "table_c"),
            self._make_row("registerTable", "ns1", "table_d"),
            self._make_row("renameTable", "ns2", "table_e"),
        ]
        cs = build_changeset_from_rows(rows, managed)

        assert cs.raw_event_count == 5
        assert cs.affected_tables("ns1") == {"table_a", "table_b", "table_d"}
        assert cs.affected_tables("ns2") == {"table_c", "table_e"}

    def test_namespace_events(self):
        managed = {"ns1"}
        rows = [
            self._make_row("CreateNamespaceEvent", "ns1"),
            self._make_row("UpdateNamespacePropertiesEvent", "ns1"),
        ]
        cs = build_changeset_from_rows(rows, managed)

        assert cs.has_namespace_changes("ns1")
        assert cs.affected_namespaces == {"ns1"}

    def test_filters_unmanaged_namespaces(self):
        managed = {"ns1"}
        rows = [
            self._make_row("createTable", "ns1", "table_a"),
            self._make_row("createTable", "unmanaged", "table_b"),
        ]
        cs = build_changeset_from_rows(rows, managed)

        assert cs.affected_namespaces == {"ns1"}
        assert cs.affected_tables("ns1") == {"table_a"}

    def test_skips_non_insert_ops(self):
        managed = {"ns1"}
        rows = [
            self._make_row("createTable", "ns1", "table_a", op="Insert"),
            self._make_row("createTable", "ns1", "table_b", op="Delete"),
            self._make_row("createTable", "ns1", "table_c", op="UpdateDelete"),
        ]
        cs = build_changeset_from_rows(rows, managed)

        assert cs.affected_tables("ns1") == {"table_a"}

    def test_accepts_numeric_op_codes(self):
        managed = {"ns1"}
        rows = [
            self._make_row("createTable", "ns1", "table_a", op=1),  # Insert
            self._make_row("createTable", "ns1", "table_b", op=3),  # UpdateInsert
            self._make_row("createTable", "ns1", "table_c", op=2),  # Delete
        ]
        cs = build_changeset_from_rows(rows, managed)

        assert cs.affected_tables("ns1") == {"table_a", "table_b"}

    def test_skips_unrecognized_event_types(self):
        managed = {"ns1"}
        rows = [
            self._make_row("loadTable", "ns1", "table_a"),
            self._make_row("unknown_event", "ns1", "table_b"),
        ]
        cs = build_changeset_from_rows(rows, managed)

        assert cs.is_empty

    def test_empty_rows(self):
        cs = build_changeset_from_rows([], {"ns1"})
        assert cs.is_empty
        assert cs.raw_event_count == 0


class TestCursorPersistence:
    def test_save_and_load(self, tmp_path):
        cursor_file = str(tmp_path / "cursor.json")
        _save_cursor_timestamp(cursor_file, 1234567890)
        assert _load_cursor_timestamp(cursor_file) == 1234567890

    def test_load_nonexistent(self, tmp_path):
        cursor_file = str(tmp_path / "nonexistent.json")
        assert _load_cursor_timestamp(cursor_file) is None

    def test_load_corrupt_file(self, tmp_path):
        cursor_file = tmp_path / "cursor.json"
        cursor_file.write_text("not json")
        assert _load_cursor_timestamp(str(cursor_file)) is None

    def test_overwrite_cursor(self, tmp_path):
        cursor_file = str(tmp_path / "cursor.json")
        _save_cursor_timestamp(cursor_file, 100)
        _save_cursor_timestamp(cursor_file, 200)
        assert _load_cursor_timestamp(cursor_file) == 200
