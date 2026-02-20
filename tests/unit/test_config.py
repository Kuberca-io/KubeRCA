"""Tests for kuberca.config — environment variable loading and validation.

Covers:
  - Default values when no KUBERCA_* env vars are set
  - Each config field read from its corresponding KUBERCA_* env var
  - Numeric clamping (min/max bounds for int fields)
  - Invalid values raise ValueError for validated fields
  - Boolean parsing for various truthy/falsy strings
"""

from __future__ import annotations

import pytest

from kuberca.config import load_config
from kuberca.models.config import KubeRCAConfig

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


class TestConfigDefaults:
    def test_returns_kuberca_config_type(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_CLUSTER_ID", raising=False)
        config = load_config()
        assert isinstance(config, KubeRCAConfig)

    def test_ollama_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_OLLAMA_ENABLED", raising=False)
        config = load_config()
        assert config.ollama.enabled is False

    def test_ollama_default_endpoint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_OLLAMA_ENDPOINT", raising=False)
        config = load_config()
        assert config.ollama.endpoint == "http://localhost:11434"

    def test_ollama_default_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_OLLAMA_MODEL", raising=False)
        config = load_config()
        assert config.ollama.model == "qwen2.5:7b"

    def test_ollama_default_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_OLLAMA_TIMEOUT", raising=False)
        config = load_config()
        assert config.ollama.timeout_seconds == 30

    def test_rule_engine_default_time_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_RULE_ENGINE_TIME_WINDOW", raising=False)
        config = load_config()
        assert config.rule_engine.time_window == "2h"

    def test_rule_engine_tier2_enabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_RULE_ENGINE_TIER2_ENABLED", raising=False)
        config = load_config()
        assert config.rule_engine.tier2_enabled is True

    def test_change_ledger_default_max_versions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_CHANGE_LEDGER_MAX_VERSIONS", raising=False)
        config = load_config()
        assert config.change_ledger.max_versions == 5

    def test_change_ledger_default_retention(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_CHANGE_LEDGER_RETENTION", raising=False)
        config = load_config()
        assert config.change_ledger.retention == "6h"

    def test_persistence_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_LEDGER_PERSISTENCE_ENABLED", raising=False)
        config = load_config()
        assert config.change_ledger.persistence_enabled is False

    def test_api_default_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_API_PORT", raising=False)
        config = load_config()
        assert config.api.port == 8080

    def test_log_default_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_LOG_LEVEL", raising=False)
        config = load_config()
        assert config.log.level == "info"

    def test_cluster_id_empty_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KUBERCA_CLUSTER_ID", raising=False)
        config = load_config()
        assert config.cluster_id == ""


# ---------------------------------------------------------------------------
# Custom env var values
# ---------------------------------------------------------------------------


class TestConfigCustomValues:
    def test_cluster_id_read_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_CLUSTER_ID", "prod-cluster-east")
        config = load_config()
        assert config.cluster_id == "prod-cluster-east"

    def test_ollama_enabled_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_ENABLED", "true")
        config = load_config()
        assert config.ollama.enabled is True

    def test_ollama_enabled_via_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_ENABLED", "1")
        config = load_config()
        assert config.ollama.enabled is True

    def test_ollama_enabled_via_yes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_ENABLED", "yes")
        config = load_config()
        assert config.ollama.enabled is True

    def test_ollama_disabled_via_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_ENABLED", "false")
        config = load_config()
        assert config.ollama.enabled is False

    def test_ollama_endpoint_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_ENDPOINT", "http://ollama.svc:11434")
        config = load_config()
        assert config.ollama.endpoint == "http://ollama.svc:11434"

    def test_ollama_model_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_MODEL", "llama3.1:8b")
        config = load_config()
        assert config.ollama.model == "llama3.1:8b"

    def test_ollama_max_retries_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_MAX_RETRIES", "2")
        config = load_config()
        assert config.ollama.max_retries == 2

    def test_api_port_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_API_PORT", "9090")
        config = load_config()
        assert config.api.port == 9090

    def test_log_level_debug(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_LOG_LEVEL", "debug")
        config = load_config()
        assert config.log.level == "debug"

    def test_log_level_uppercase_normalised(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_LOG_LEVEL", "WARNING")
        config = load_config()
        assert config.log.level == "warning"

    def test_persistence_enabled_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_LEDGER_PERSISTENCE_ENABLED", "true")
        config = load_config()
        assert config.change_ledger.persistence_enabled is True

    def test_change_ledger_max_versions_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_CHANGE_LEDGER_MAX_VERSIONS", "10")
        config = load_config()
        assert config.change_ledger.max_versions == 10


# ---------------------------------------------------------------------------
# Numeric clamping
# ---------------------------------------------------------------------------


class TestConfigClamping:
    def test_ollama_timeout_clamped_to_minimum_10(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_TIMEOUT", "5")
        config = load_config()
        assert config.ollama.timeout_seconds == 10

    def test_ollama_timeout_clamped_to_maximum_90(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_TIMEOUT", "120")
        config = load_config()
        assert config.ollama.timeout_seconds == 90

    def test_api_port_clamped_to_minimum_1024(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_API_PORT", "80")
        config = load_config()
        assert config.api.port == 1024

    def test_api_port_clamped_to_maximum_65535(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_API_PORT", "70000")
        config = load_config()
        assert config.api.port == 65535

    def test_change_ledger_max_versions_clamped_to_minimum_2(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_CHANGE_LEDGER_MAX_VERSIONS", "1")
        config = load_config()
        assert config.change_ledger.max_versions == 2

    def test_change_ledger_max_versions_clamped_to_maximum_20(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_CHANGE_LEDGER_MAX_VERSIONS", "50")
        config = load_config()
        assert config.change_ledger.max_versions == 20

    def test_ollama_max_retries_clamped_to_minimum_0(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_MAX_RETRIES", "-1")
        config = load_config()
        assert config.ollama.max_retries == 0

    def test_ollama_max_retries_clamped_to_maximum_5(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_OLLAMA_MAX_RETRIES", "10")
        config = load_config()
        assert config.ollama.max_retries == 5


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_invalid_time_window_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_RULE_ENGINE_TIME_WINDOW", "xyz")
        with pytest.raises(ValueError, match="Invalid time window format"):
            load_config()

    def test_invalid_retention_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_CHANGE_LEDGER_RETENTION", "forever")
        with pytest.raises(ValueError, match="Invalid time window format"):
            load_config()

    def test_invalid_log_level_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBERCA_LOG_LEVEL", "trace")
        with pytest.raises(ValueError, match="Invalid log level"):
            load_config()

    def test_valid_time_window_formats_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for valid in ("30m", "2h", "1d"):
            monkeypatch.setenv("KUBERCA_RULE_ENGINE_TIME_WINDOW", valid)
            config = load_config()
            assert config.rule_engine.time_window == valid

    def test_valid_log_levels_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for level in ("debug", "info", "warning", "error"):
            monkeypatch.setenv("KUBERCA_LOG_LEVEL", level)
            config = load_config()
            assert config.log.level == level
