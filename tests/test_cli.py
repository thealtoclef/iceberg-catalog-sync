"""Tests for the CLI entrypoint."""

from __future__ import annotations

import textwrap
from unittest.mock import patch

from click.testing import CliRunner

from iceberg_catalog_sync.cli import main
from iceberg_catalog_sync.reporting import SyncError, SyncResult


class TestCLI:
    def test_missing_config_arg(self):
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert result.exit_code != 0

    def test_nonexistent_config_file(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--config", "/nonexistent/config.yaml"])
        assert result.exit_code != 0

    def test_successful_sync(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
        """)
        )

        mock_result = SyncResult()
        with patch("iceberg_catalog_sync.cli.sync_catalogs", return_value=mock_result):
            runner = CliRunner()
            result = runner.invoke(main, ["--config", str(config_file)])
            assert result.exit_code == 0
            assert "complete" in result.output.lower()

    def test_sync_with_errors_exits_1(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
        """)
        )

        mock_result = SyncResult(
            errors=[SyncError(namespace="test_ns", table="bad", error="failed")]
        )
        with patch("iceberg_catalog_sync.cli.sync_catalogs", return_value=mock_result):
            runner = CliRunner()
            result = runner.invoke(main, ["--config", str(config_file)])
            assert result.exit_code == 1

    def test_explicit_full_mode(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
        """)
        )

        mock_result = SyncResult()
        with patch("iceberg_catalog_sync.cli.sync_catalogs", return_value=mock_result):
            runner = CliRunner()
            result = runner.invoke(main, ["--config", str(config_file), "--mode", "full"])
            assert result.exit_code == 0
            assert "full mode" in result.output.lower()

    def test_events_mode_requires_enabled(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
        """)
        )

        runner = CliRunner()
        result = runner.invoke(main, ["--config", str(config_file), "--mode", "events"])
        assert result.exit_code == 1
        assert "events.enabled" in result.output

    def test_events_mode_no_pending_events(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
            events:
              enabled: true
        """)
        )

        mock_events = {
            "setup_risingwave": lambda config: False,
            "consume_events": lambda config: ([], None),
            "build_changeset_from_rows": lambda rows, exclude_namespaces=None: type(
                "MockChangeSet", (), {"is_empty": True, "raw_event_count": 0}
            )(),
            "save_cursor": lambda config, ts: None,
        }

        with patch.dict("sys.modules", {"iceberg_catalog_sync.events": type("MockEvents", (), mock_events)}):
            runner = CliRunner()
            result = runner.invoke(main, ["--config", str(config_file), "--mode", "events"])
            assert result.exit_code == 0
            assert "no pending events" in result.output.lower()

    def test_events_mode_cursor_not_advanced_on_failure(self, tmp_path):
        """When sync has errors, cursor should NOT be advanced."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
            events:
              enabled: true
        """)
        )

        rows = [{"op": "Insert", "type": "createTable", "namespace": "ns1",
                 "name": "t1", "rw_timestamp": 100}]
        failed_result = SyncResult(
            errors=[SyncError(namespace="ns1", table="t1", error="boom")]
        )

        mock_events = {
            "setup_risingwave": lambda config: False,
            "consume_events": lambda config: (rows, 100),
            "build_changeset_from_rows": lambda rows, exclude_namespaces=None: type(
                "MockChangeSet", (), {
                    "is_empty": False,
                    "affected_namespaces": {"ns1"},
                    "affected_tables": lambda ns: {"t1"},
                }
            )(),
            "save_cursor": lambda config, ts: None,
        }

        with (
            patch("iceberg_catalog_sync.cli.sync_from_changeset", return_value=failed_result),
            patch.dict("sys.modules", {"iceberg_catalog_sync.events": type("MockEvents", (), mock_events)}),
        ):
            mock_module = __import__("iceberg_catalog_sync.events")
            with patch.object(mock_module.events, "save_cursor") as mock_save:
                runner = CliRunner()
                result = runner.invoke(main, ["--config", str(config_file), "--mode", "events"])
                assert result.exit_code == 1
                mock_save.assert_not_called()

    def test_events_mode_initial_run_triggers_full_sync(self, tmp_path):
        """On initial run (subscription doesn't exist), full sync runs first."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
            events:
              enabled: true
        """)
        )

        rows = [{"op": "Insert", "type": "createTable", "namespace": "ns1",
                 "name": "t1", "rw_timestamp": 100}]
        full_result = SyncResult()
        partial_result = SyncResult()

        mock_events = {
            "setup_risingwave": lambda config: True,
            "consume_events": lambda config: (rows, 100),
            "build_changeset_from_rows": lambda rows, exclude_namespaces=None: type(
                "MockChangeSet", (), {
                    "is_empty": False,
                    "affected_namespaces": {"ns1"},
                    "affected_tables": lambda ns: {"t1"},
                }
            )(),
            "save_cursor": lambda config, ts: None,
        }

        with (
            patch("iceberg_catalog_sync.cli.sync_catalogs", return_value=full_result) as mock_full,
            patch("iceberg_catalog_sync.cli.sync_from_changeset", return_value=partial_result),
            patch.dict("sys.modules", {"iceberg_catalog_sync.events": type("MockEvents", (), mock_events)}),
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["--config", str(config_file), "--mode", "events"])
            assert result.exit_code == 0
            mock_full.assert_called_once()
            assert "initial run" in result.output.lower()
            assert "full sync" in result.output.lower()

    def test_events_mode_initial_run_full_sync_failure_aborts(self, tmp_path):
        """If full sync fails on initial run, event mode is aborted."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh
            events:
              enabled: true
        """)
        )

        rows = [{"op": "Insert", "type": "createTable", "namespace": "ns1",
                 "name": "t1", "rw_timestamp": 100}]
        failed_full_result = SyncResult(
            errors=[SyncError(namespace="ns1", table="t1", error="initial sync failed")]
        )

        mock_events = {
            "setup_risingwave": lambda config: True,
            "consume_events": lambda config: (rows, 100),
            "build_changeset_from_rows": lambda rows, exclude_namespaces=None: type(
                "MockChangeSet", (), {
                    "is_empty": False,
                    "affected_namespaces": {"ns1"},
                    "affected_tables": lambda ns: {"t1"},
                }
            )(),
            "save_cursor": lambda config, ts: None,
        }

        with (
            patch("iceberg_catalog_sync.cli.sync_catalogs", return_value=failed_full_result),
            patch("iceberg_catalog_sync.cli.sync_from_changeset") as mock_partial,
            patch.dict("sys.modules", {"iceberg_catalog_sync.events": type("MockEvents", (), mock_events)}),
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["--config", str(config_file), "--mode", "events"])
            assert result.exit_code == 1
            mock_partial.assert_not_called()
            assert "aborted" in result.output.lower()
