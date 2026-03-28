"""Tests for config loading and validation."""

from __future__ import annotations

import os
import textwrap

import pytest

from iceberg_catalog_sync.config import AppConfig, LogConfig, load_config


class TestLoadConfig:
    def test_load_minimal_config(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://localhost:8181
                warehouse: wh1
              destination:
                name: dst
                uri: http://localhost:8182
                warehouse: wh2
            sync:
              namespaces:
                - my_db
        """)
        )
        config = load_config(str(config_file))
        assert config.catalogs.source.name == "src"
        assert config.catalogs.destination.uri == "http://localhost:8182"
        assert config.sync.namespaces == ["my_db"]
        assert config.sync.dry_run is False
        assert config.retry.max_attempts == 5

    def test_env_var_expansion(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_URI", "http://from-env:8181")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: ${TEST_URI}
                warehouse: wh
              destination:
                name: dst
                uri: http://dest:8181
                warehouse: wh
            sync:
              namespaces: [db1]
        """)
        )
        config = load_config(str(config_file))
        assert config.catalogs.source.uri == "http://from-env:8181"

    def test_env_var_with_default(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: ${NONEXISTENT_VAR:-http://default:8181}
                warehouse: wh
              destination:
                name: dst
                uri: http://dest:8181
                warehouse: wh
            sync:
              namespaces: [db1]
        """)
        )
        # Ensure the var is not set
        os.environ.pop("NONEXISTENT_VAR", None)
        config = load_config(str(config_file))
        assert config.catalogs.source.uri == "http://default:8181"

    def test_missing_env_var_raises(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: ${MISSING_REQUIRED_VAR}
                warehouse: wh
              destination:
                name: dst
                uri: http://dest:8181
                warehouse: wh
            sync:
              namespaces: [db1]
        """)
        )
        os.environ.pop("MISSING_REQUIRED_VAR", None)
        with pytest.raises(ValueError, match="MISSING_REQUIRED_VAR"):
            load_config(str(config_file))

    def test_full_config(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            catalogs:
              source:
                name: src
                uri: http://src:8181
                warehouse: wh
                properties:
                  token: abc123
              destination:
                name: dst
                uri: http://dst:8181
                warehouse: wh2
            sync:
              namespaces: [db1, db2]
              dry_run: true
              drop_orphan_tables: true
              sync_namespace_properties: false
            retry:
              max_attempts: 10
              base_delay_seconds: 2.0
              max_delay_seconds: 120.0
            log:
              level: DEBUG
              format: json
            events:
              enabled: true
              risingwave:
                host: rw.example.com
                port: 5432
                user: myuser
                password: mypass
                database: mydb
                subscription_name: my_sub
                cursor_path: /tmp/cursor.json
              max_events: 500
            metrics:
              enabled: true
              exporter: console
            tracing:
              enabled: true
              exporter: console
              service_name: my-sync
        """)
        )
        config = load_config(str(config_file))
        assert config.catalogs.source.properties == {"token": "abc123"}
        assert config.sync.dry_run is True
        assert config.sync.drop_orphan_tables is True
        assert config.retry.max_attempts == 10
        assert config.log.format == "json"
        assert config.events.enabled is True
        assert config.events.risingwave.host == "rw.example.com"
        assert config.events.risingwave.subscription_name == "my_sub"
        assert config.events.max_events == 500
        assert config.metrics.enabled is True
        assert config.tracing.service_name == "my-sync"


class TestLogConfig:
    def test_valid_levels(self):
        for level in ("debug", "INFO", "Warning", "ERROR", "critical"):
            cfg = LogConfig(level=level)
            assert cfg.level == level.upper()

    def test_invalid_level(self):
        with pytest.raises(ValueError, match="Invalid log level"):
            LogConfig(level="TRACE")

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid log format"):
            LogConfig(format="xml")
