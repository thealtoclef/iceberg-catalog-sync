"""Tests for config loading and validation."""

from __future__ import annotations

import os
import textwrap

import pytest

from iceberg_catalog_sync.config import AppConfig, LogConfig, RisingWaveConfig, load_config


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
        """)
        )
        config = load_config(str(config_file))
        assert config.catalogs.source.name == "src"
        assert config.catalogs.destination.uri == "http://localhost:8182"
        assert config.sync.exclude_namespaces == []
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
        """)
        )
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
              exclude_namespaces: [system]
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
                schema_name: analytics
                source_table: my_events_raw
              sync_interval_seconds: 30
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
        assert config.sync.exclude_namespaces == ["system"]
        assert config.retry.max_attempts == 10
        assert config.log.format == "json"
        assert config.events.enabled is True
        rw = config.events.risingwave
        assert rw.host == "rw.example.com"
        assert rw.schema_name == "analytics"
        assert rw.source_table == "my_events_raw"
        assert rw.mv_name == "__ics_my_events_raw_mv"
        assert rw.subscription_name == "__ics_my_events_raw_sub"
        assert rw.progress_table == "__ics_my_events_raw_progress"
        assert config.events.sync_interval_seconds == 30
        assert config.events.max_events == 500
        assert config.metrics.enabled is True
        assert config.tracing.service_name == "my-sync"


class TestRisingWaveConfig:
    def test_derived_names_default(self):
        rw = RisingWaveConfig()
        assert rw.mv_name == "__ics_lakekeeper_events_raw_mv"
        assert rw.subscription_name == "__ics_lakekeeper_events_raw_sub"
        assert rw.progress_table == "__ics_lakekeeper_events_raw_progress"

    def test_derived_names_custom(self):
        rw = RisingWaveConfig(source_table="my_webhook")
        assert rw.mv_name == "__ics_my_webhook_mv"
        assert rw.subscription_name == "__ics_my_webhook_sub"
        assert rw.progress_table == "__ics_my_webhook_progress"

    def test_retention_default(self):
        rw = RisingWaveConfig()
        assert rw.retention == "1 day"

    def test_retention_custom(self):
        rw = RisingWaveConfig(retention="7 days")
        assert rw.retention == "7 days"


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
