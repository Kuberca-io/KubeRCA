"""Unit tests for kuberca.cli.main."""

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner

from kuberca import __version__
from kuberca.cli.main import cli

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _status_response() -> dict[object, object]:
    return {
        "pod_counts": {"Running": 5, "Pending": 1},
        "node_conditions": [{"name": "node-1", "type": "Ready", "status": "True", "reason": ""}],
        "active_anomalies": [],
        "recent_events": [
            {
                "namespace": "default",
                "resource_kind": "Pod",
                "resource_name": "my-pod",
                "reason": "Started",
                "message": "Started container api",
                "severity": "info",
                "last_seen": "2024-01-15T10:30:00+00:00",
                "count": 1,
            }
        ],
        "cache_state": "ready",
    }


def _rca_response() -> dict[object, object]:
    return {
        "root_cause": "Image pull failure.",
        "confidence": 0.92,
        "diagnosed_by": "rule_engine",
        "rule_id": "image-pull-failure",
        "evidence": [
            {
                "type": "event",
                "timestamp": "2024-01-15T10:30:00Z",
                "summary": "ErrImagePull",
                "debug_context": None,
            }
        ],
        "affected_resources": [{"kind": "Pod", "namespace": "default", "name": "api-pod"}],
        "suggested_remediation": "Fix the image tag.",
        "meta": {
            "kuberca_version": "0.1.0",
            "schema_version": "1",
            "cluster_id": "",
            "timestamp": "2024-01-15T10:30:01Z",
            "response_time_ms": 38,
            "cache_state": "ready",
            "warnings": [],
        },
    }


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------


class TestVersionCommand:
    def test_prints_version(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert __version__ in result.output
        assert "kuberca" in result.output


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


class TestStatusCommand:
    def test_status_pretty_output(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._get", return_value=_status_response()):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "Running" in result.output
        assert "5" in result.output
        assert "No active anomalies" in result.output

    def test_status_json_output(self) -> None:
        runner = CliRunner()
        import json

        with patch("kuberca.cli.main._get", return_value=_status_response()):
            result = runner.invoke(cli, ["status", "--json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["cache_state"] == "ready"

    def test_status_with_namespace_flag(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._get", return_value=_status_response()) as mock_get:
            result = runner.invoke(cli, ["status", "--namespace", "production"])
        assert result.exit_code == 0
        call_args = mock_get.call_args
        assert call_args[1]["params"]["namespace"] == "production"

    def test_status_connection_error_exits_nonzero(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._get", side_effect=Exception("cannot connect")):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code != 0

    def test_status_shows_unhealthy_node_conditions(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["node_conditions"] = [  # type: ignore[index]
            {"name": "node-1", "type": "Ready", "status": "False", "reason": "KubeletNotReady"}
        ]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "KubeletNotReady" in result.output or "node-1" in result.output


# ---------------------------------------------------------------------------
# analyze
# ---------------------------------------------------------------------------


class TestAnalyzeCommand:
    def test_analyze_pretty_output(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "Image pull failure." in result.output
        assert "92%" in result.output
        assert "rule_engine" in result.output

    def test_analyze_json_output(self) -> None:
        runner = CliRunner()
        import json

        with patch("kuberca.cli.main._post", return_value=_rca_response()):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod", "--json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["root_cause"] == "Image pull failure."

    def test_analyze_with_time_window(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()) as mock_post:
            result = runner.invoke(cli, ["analyze", "Deployment/prod/api", "--time-window", "30m"])
        assert result.exit_code == 0
        body = mock_post.call_args[0][2]
        assert body["time_window"] == "30m"

    def test_analyze_invalid_resource_format_exits_with_usage_error(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["analyze", "Pod/default"])
        assert result.exit_code != 0
        assert "Kind/namespace/name" in result.output

    def test_analyze_shows_evidence(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert "ErrImagePull" in result.output

    def test_analyze_shows_remediation(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert "Fix the image tag." in result.output

    def test_analyze_shows_affected_resources(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert "api-pod" in result.output

    def test_analyze_api_url_from_env(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()) as mock_post:
            result = runner.invoke(
                cli,
                ["--api-url", "http://kuberca.internal:9090", "analyze", "Pod/ns/pod"],
            )
        assert result.exit_code == 0
        called_url = mock_post.call_args[0][0]
        assert called_url == "http://kuberca.internal:9090"
