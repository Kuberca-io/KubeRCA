"""Unit tests for kuberca.cli.main."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
            "kuberca_version": "0.1.2",
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


# ---------------------------------------------------------------------------
# Cache state rendering
# ---------------------------------------------------------------------------


class TestCacheStateRendering:
    def test_status_cache_state_partially_ready(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["cache_state"] = "partially_ready"  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "partially_ready" in result.output

    def test_status_cache_state_degraded(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["cache_state"] = "degraded"  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "degraded" in result.output

    def test_status_cache_state_warming(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["cache_state"] = "warming"  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "warming" in result.output

    def test_status_cache_state_unknown_falls_back_gracefully(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["cache_state"] = "unknown_state"  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "unknown_state" in result.output

    def test_analyze_meta_cache_state_degraded(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["meta"]["cache_state"] = "degraded"  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "degraded" in result.output


# ---------------------------------------------------------------------------
# Confidence color thresholds
# ---------------------------------------------------------------------------


class TestConfidenceColorThresholds:
    def test_confidence_green_above_threshold(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["confidence"] = 0.90  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "90%" in result.output

    def test_confidence_green_at_boundary(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["confidence"] = 0.75  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "75%" in result.output

    def test_confidence_yellow_band(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["confidence"] = 0.60  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "60%" in result.output

    def test_confidence_yellow_at_boundary(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["confidence"] = 0.50  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "50%" in result.output

    def test_confidence_red_below_threshold(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["confidence"] = 0.30  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "30%" in result.output

    def test_confidence_zero(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["confidence"] = 0.0  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "0%" in result.output


# ---------------------------------------------------------------------------
# Analyze edge cases
# ---------------------------------------------------------------------------


class TestAnalyzeEdgeCases:
    def test_analyze_with_warnings_in_meta(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["meta"]["warnings"] = ["LLM fallback used", "Partial evidence only"]  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "Warning: LLM fallback used" in result.output
        assert "Warning: Partial evidence only" in result.output

    def test_analyze_with_empty_evidence_skips_evidence_block(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["evidence"] = []  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "Evidence" not in result.output

    def test_analyze_with_no_remediation_skips_remediation_block(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["suggested_remediation"] = ""  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "Suggested Remediation" not in result.output

    def test_analyze_without_rule_id_omits_rule_annotation(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["rule_id"] = None  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "rule:" not in result.output

    def test_analyze_llm_diagnosed_shows_llm_in_output(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["diagnosed_by"] = "llm"  # type: ignore[index]
        response["rule_id"] = None  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "llm" in result.output

    def test_analyze_without_meta_skips_meta_block(self) -> None:
        runner = CliRunner()
        response = _rca_response()
        response["meta"] = None  # type: ignore[index]
        with patch("kuberca.cli.main._post", return_value=response):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code == 0
        assert "Meta:" not in result.output

    def test_analyze_shows_time_window_in_preamble(self) -> None:
        runner = CliRunner()
        with patch("kuberca.cli.main._post", return_value=_rca_response()):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod", "--time-window", "2h"])
        assert result.exit_code == 0
        assert "window: 2h" in result.output


# ---------------------------------------------------------------------------
# Status with active anomalies
# ---------------------------------------------------------------------------


class TestStatusWithAnomalies:
    def test_status_shows_active_anomaly_count_header(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["active_anomalies"] = [  # type: ignore[index]
            {
                "severity": "error",
                "resource_kind": "Pod",
                "resource_name": "crash-pod",
                "namespace": "default",
                "reason": "CrashLoopBackOff",
            }
        ]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "Active Anomalies (1)" in result.output

    def test_status_shows_anomaly_details(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["active_anomalies"] = [  # type: ignore[index]
            {
                "severity": "critical",
                "resource_kind": "Node",
                "resource_name": "node-2",
                "namespace": "kube-system",
                "reason": "NodeNotReady",
            }
        ]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "Node/node-2" in result.output
        assert "NodeNotReady" in result.output
        assert "kube-system" in result.output

    def test_status_shows_multiple_anomalies(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["active_anomalies"] = [  # type: ignore[index]
            {
                "severity": "warning",
                "resource_kind": "Pod",
                "resource_name": "pod-a",
                "namespace": "ns-a",
                "reason": "OOMKilled",
            },
            {
                "severity": "error",
                "resource_kind": "Pod",
                "resource_name": "pod-b",
                "namespace": "ns-b",
                "reason": "ImagePullBackOff",
            },
        ]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "Active Anomalies (2)" in result.output
        assert "OOMKilled" in result.output
        assert "ImagePullBackOff" in result.output


# ---------------------------------------------------------------------------
# Status with many events (truncation)
# ---------------------------------------------------------------------------


class TestStatusManyEvents:
    def _make_event(self, index: int) -> dict[str, object]:
        return {
            "namespace": "default",
            "resource_kind": "Pod",
            "resource_name": f"pod-{index}",
            "reason": "Started",
            "message": f"Started container #{index}",
            "severity": "info",
            "last_seen": "2024-01-15T10:30:00+00:00",
            "count": 1,
        }

    def test_status_shows_header_with_total_count_when_more_than_ten(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["recent_events"] = [self._make_event(i) for i in range(15)]  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "15" in result.output
        assert "Recent Events" in result.output

    def test_status_renders_only_ten_events_when_fifteen_provided(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["recent_events"] = [self._make_event(i) for i in range(15)]  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        # pod-9 is the 10th (index 9), pod-10 is the 11th — must be absent
        assert "pod-9" in result.output
        assert "pod-10" not in result.output

    def test_status_renders_all_events_when_exactly_ten(self) -> None:
        runner = CliRunner()
        response = _status_response()
        response["recent_events"] = [self._make_event(i) for i in range(10)]  # type: ignore[index]
        with patch("kuberca.cli.main._get", return_value=response):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "pod-9" in result.output


# ---------------------------------------------------------------------------
# HTTP error handling
# ---------------------------------------------------------------------------


def _make_http_error_mock(method: str, url: str, status_code: int, body: str, content_type: str = "text/plain") -> object:
    """Build a mock httpx.Client that raises HTTPStatusError for the given response."""
    from unittest.mock import MagicMock

    import httpx

    req = httpx.Request(method, url)
    resp = httpx.Response(
        status_code,
        request=req,
        text=body,
        headers={"content-type": content_type},
    )
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        str(status_code), request=req, response=resp
    )
    mock_client.get.return_value = mock_response
    mock_client.post.return_value = mock_response
    return mock_client


class TestHttpErrorHandling:
    def test_get_json_error_body_shown_in_output(self) -> None:
        """_get parses JSON error body and raises ClickException with error/detail fields."""
        import json

        runner = CliRunner()
        mock_client = _make_http_error_mock(
            "GET",
            "http://localhost:8080/api/v1/status",
            404,
            json.dumps({"error": "NOT_FOUND", "detail": "Namespace not found"}),
            "application/json",
        )
        with patch("httpx.Client", return_value=mock_client):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code != 0
        assert "NOT_FOUND" in result.output
        assert "Namespace not found" in result.output

    def test_get_plain_text_error_body_shown_in_output(self) -> None:
        """_get falls back to HTTP status code message when body is not JSON."""
        runner = CliRunner()
        mock_client = _make_http_error_mock(
            "GET",
            "http://localhost:8080/api/v1/status",
            503,
            "Service Unavailable",
        )
        with patch("httpx.Client", return_value=mock_client):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code != 0
        assert "HTTP 503" in result.output
        assert "Service Unavailable" in result.output

    def test_post_json_error_body_shown_in_output(self) -> None:
        """_post parses JSON error body and raises ClickException with error/detail fields."""
        import json

        runner = CliRunner()
        mock_client = _make_http_error_mock(
            "POST",
            "http://localhost:8080/api/v1/analyze",
            422,
            json.dumps({"error": "VALIDATION_ERROR", "detail": "Invalid resource format"}),
            "application/json",
        )
        with patch("httpx.Client", return_value=mock_client):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code != 0
        assert "VALIDATION_ERROR" in result.output
        assert "Invalid resource format" in result.output

    def test_post_plain_text_error_body_shown_in_output(self) -> None:
        """_post falls back to HTTP status code message when body is not JSON."""
        runner = CliRunner()
        mock_client = _make_http_error_mock(
            "POST",
            "http://localhost:8080/api/v1/analyze",
            500,
            "Internal Server Error",
        )
        with patch("httpx.Client", return_value=mock_client):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code != 0
        assert "HTTP 500" in result.output
        assert "Internal Server Error" in result.output

    def test_connect_error_on_status_shows_friendly_message(self) -> None:
        """_get raises ClickException with a user-friendly message on ConnectError."""
        import httpx

        runner = CliRunner()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        req = httpx.Request("GET", "http://localhost:8080/api/v1/status")
        mock_client.get.side_effect = httpx.ConnectError("Connection refused", request=req)
        with patch("httpx.Client", return_value=mock_client):
            result = runner.invoke(cli, ["status"])
        assert result.exit_code != 0
        assert "Cannot connect" in result.output

    def test_connect_error_on_analyze_shows_friendly_message(self) -> None:
        """_post raises ClickException with a user-friendly message on ConnectError."""
        import httpx

        runner = CliRunner()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        req = httpx.Request("POST", "http://localhost:8080/api/v1/analyze")
        mock_client.post.side_effect = httpx.ConnectError("Connection refused", request=req)
        with patch("httpx.Client", return_value=mock_client):
            result = runner.invoke(cli, ["analyze", "Pod/default/api-pod"])
        assert result.exit_code != 0
        assert "Cannot connect" in result.output


# ---------------------------------------------------------------------------
# Additional invalid resource format variations
# ---------------------------------------------------------------------------


class TestInvalidResourceFormats:
    def test_too_many_parts_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["analyze", "Pod/default/api-pod/extra"])
        assert result.exit_code != 0
        assert "Kind/namespace/name" in result.output

    def test_no_slashes_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["analyze", "my-pod"])
        assert result.exit_code != 0
        assert "Kind/namespace/name" in result.output

    def test_empty_kind_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["analyze", "/default/api-pod"])
        assert result.exit_code != 0
        assert "Kind/namespace/name" in result.output

    def test_empty_name_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(cli, ["analyze", "Pod/default/"])
        assert result.exit_code != 0
        assert "Kind/namespace/name" in result.output
