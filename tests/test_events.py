"""Tests for event consumption and ChangeSet building."""

from __future__ import annotations

import pytest

from iceberg_catalog_sync.events import (
    ChangeSet,
    build_changeset_from_rows,
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
        rows = [
            self._make_row("createTable", "ns1", "table_a"),
            self._make_row("updateTable", "ns1", "table_b"),
            self._make_row("dropTable", "ns2", "table_c"),
            self._make_row("registerTable", "ns1", "table_d"),
            self._make_row("renameTable", "ns2", "table_e"),
        ]
        cs = build_changeset_from_rows(rows)

        assert cs.raw_event_count == 5
        assert cs.affected_tables("ns1") == {"table_a", "table_b", "table_d"}
        assert cs.affected_tables("ns2") == {"table_c", "table_e"}

    def test_namespace_events(self):
        rows = [
            self._make_row("CreateNamespaceEvent", "ns1"),
            self._make_row("UpdateNamespacePropertiesEvent", "ns1"),
        ]
        cs = build_changeset_from_rows(rows)

        assert cs.has_namespace_changes("ns1")
        assert cs.affected_namespaces == {"ns1"}

    def test_filters_excluded_namespaces(self):
        rows = [
            self._make_row("createTable", "ns1", "table_a"),
            self._make_row("createTable", "excluded", "table_b"),
        ]
        cs = build_changeset_from_rows(rows, exclude_namespaces={"excluded"})

        assert cs.affected_namespaces == {"ns1"}
        assert cs.affected_tables("ns1") == {"table_a"}

    def test_no_exclusions_passes_all(self):
        rows = [
            self._make_row("createTable", "ns1", "table_a"),
            self._make_row("createTable", "ns2", "table_b"),
        ]
        cs = build_changeset_from_rows(rows)

        assert cs.affected_namespaces == {"ns1", "ns2"}

    def test_skips_non_insert_ops(self):
        rows = [
            self._make_row("createTable", "ns1", "table_a", op="Insert"),
            self._make_row("createTable", "ns1", "table_b", op="Delete"),
            self._make_row("createTable", "ns1", "table_c", op="UpdateDelete"),
        ]
        cs = build_changeset_from_rows(rows)

        assert cs.affected_tables("ns1") == {"table_a"}

    def test_accepts_numeric_op_codes(self):
        rows = [
            self._make_row("createTable", "ns1", "table_a", op=1),  # Insert
            self._make_row("createTable", "ns1", "table_b", op=3),  # UpdateInsert
            self._make_row("createTable", "ns1", "table_c", op=2),  # Delete
        ]
        cs = build_changeset_from_rows(rows)

        assert cs.affected_tables("ns1") == {"table_a", "table_b"}

    def test_skips_unrecognized_event_types(self):
        rows = [
            self._make_row("loadTable", "ns1", "table_a"),
            self._make_row("unknown_event", "ns1", "table_b"),
        ]
        cs = build_changeset_from_rows(rows)

        assert cs.is_empty

    def test_empty_rows(self):
        cs = build_changeset_from_rows([])
        assert cs.is_empty
        assert cs.raw_event_count == 0


class TestTupleToDict:
    def test_converts_mv_tuple(self):
        from iceberg_catalog_sync.events import _tuple_to_dict

        row = (
            "event-1", "createTable", "uri:lk:host",
            "my_ns", "my_table", "Table/uuid-1",
            "wh-uuid", "trace-1", '{"key": "val"}',
            "Insert", 1234567890,
        )
        d = _tuple_to_dict(row)
        assert d["type"] == "createTable"
        assert d["namespace"] == "my_ns"
        assert d["name"] == "my_table"
        assert d["op"] == "Insert"
        assert d["rw_timestamp"] == 1234567890
