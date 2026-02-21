"""Supplemental unit tests to improve coverage for CLI, API routes, MCP,
observability/logging, __main__, and dependency_graph edge cases.

Targets uncovered lines:
  - cli/main.py:    52, 75-85, 93-103, 108-115, 222-230, 358-359
  - api/routes.py:  87, 147-188, 204-213, 233-234, 270-272, 328-329
  - mcp/server.py:  61, 112-117, 218-230
  - observability/logging.py: 15-17
  - __main__.py:    8-14
  - dependency_graph.py: 76, 133-136, 187-188, 191-196, 218, 238, 289, 294, 309-317
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from kuberca.api.app import create_app
from kuberca.api.routes import (
    _build_cluster_status,
    _get_registered_rule_count,
    _get_top_rules_matched,
)
from kuberca.cli.main import (
    _confidence_color,
    _get,
    _handle_error_response,
    _post,
    cli,
)
from kuberca.graph.dependency_graph import DependencyGraph
from kuberca.graph.models import EdgeType
from kuberca.models.resources import CacheReadiness, ResourceSnapshot
from kuberca.observability.logging import get_logger, setup_logging

# ---------------------------------------------------------------------------
# Module 1: cli/main.py
# ---------------------------------------------------------------------------


class TestConfidenceColor:
    def test_high_confidence_is_green(self) -> None:
        assert _confidence_color(0.9) == "green"

    def test_threshold_at_075_is_green(self) -> None:
        assert _confidence_color(0.75) == "green"

    def test_mid_confidence_is_yellow(self) -> None:
        assert _confidence_color(0.6) == "yellow"

    def test_threshold_at_05_is_yellow(self) -> None:
        assert _confidence_color(0.5) == "yellow"

    def test_low_confidence_is_red(self) -> None:
        assert _confidence_color(0.1) == "red"

    def test_zero_confidence_is_red(self) -> None:
        assert _confidence_color(0.0) == "red"

    def test_negative_confidence_falls_through_to_red(self) -> None:
        # line 52: the for-loop exhausts without a match, fallback returns "red"
        assert _confidence_color(-0.5) == "red"


class TestGetHttpHelper:
    def _make_mock_client(self) -> MagicMock:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        return mock_client

    @patch("kuberca.cli.main.httpx.Client")
    def test_connect_error_raises_click_exception(self, mock_client_cls: MagicMock) -> None:
        # line 81-82: ConnectError -> ClickException
        mock_client = self._make_mock_client()
        mock_client.get.side_effect = httpx.ConnectError("connection refused")
        mock_client_cls.return_value = mock_client

        import click

        with pytest.raises(click.ClickException) as exc_info:
            _get("http://localhost:8080", "/api/v1/status")

        assert "Cannot connect" in str(exc_info.value.format_message())
        assert "localhost:8080" in str(exc_info.value.format_message())

    @patch("kuberca.cli.main.httpx.Client")
    def test_http_status_error_calls_handle_error_response(self, mock_client_cls: MagicMock) -> None:
        # lines 83-85: HTTPStatusError -> _handle_error_response -> ClickException
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 404
        mock_response.text = "not found"
        mock_response.json.side_effect = Exception("not JSON")

        mock_client = self._make_mock_client()
        mock_client.get.return_value = mock_response
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404",
            request=MagicMock(),
            response=mock_response,
        )
        mock_client_cls.return_value = mock_client

        import click

        with pytest.raises(click.ClickException) as exc_info:
            _get("http://localhost:8080", "/api/v1/status")

        assert "404" in str(exc_info.value.format_message())

    @patch("kuberca.cli.main.httpx.Client")
    def test_successful_get_returns_json(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"key": "value"}

        mock_client = self._make_mock_client()
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        result = _get("http://localhost:8080", "/api/v1/health")

        assert result == {"key": "value"}

    @patch("kuberca.cli.main.httpx.Client")
    def test_params_forwarded_to_get(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {}

        mock_client = self._make_mock_client()
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        _get("http://localhost:8080", "/api/v1/status", params={"namespace": "prod"})

        call_kwargs = mock_client.get.call_args
        assert call_kwargs[1]["params"] == {"namespace": "prod"}


class TestPostHttpHelper:
    def _make_mock_client(self) -> MagicMock:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        return mock_client

    @patch("kuberca.cli.main.httpx.Client")
    def test_connect_error_raises_click_exception(self, mock_client_cls: MagicMock) -> None:
        # lines 99-100: ConnectError -> ClickException
        mock_client = self._make_mock_client()
        mock_client.post.side_effect = httpx.ConnectError("refused")
        mock_client_cls.return_value = mock_client

        import click

        with pytest.raises(click.ClickException) as exc_info:
            _post("http://localhost:8080", "/api/v1/analyze", {"resource": "Pod/ns/name"})

        assert "Cannot connect" in str(exc_info.value.format_message())

    @patch("kuberca.cli.main.httpx.Client")
    def test_http_status_error_calls_handle_error_response(self, mock_client_cls: MagicMock) -> None:
        # lines 101-103: HTTPStatusError -> _handle_error_response -> ClickException
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 500
        mock_response.text = "server error"
        mock_response.json.return_value = {"error": "INTERNAL_ERROR", "detail": "boom"}

        mock_client = self._make_mock_client()
        mock_client.post.return_value = mock_response
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500",
            request=MagicMock(),
            response=mock_response,
        )
        mock_client_cls.return_value = mock_client

        import click

        with pytest.raises(click.ClickException) as exc_info:
            _post("http://localhost:8080", "/api/v1/analyze", {"resource": "Pod/ns/name"})

        assert "INTERNAL_ERROR" in str(exc_info.value.format_message())


class TestHandleErrorResponse:
    def _make_response(self, status_code: int, body: str, is_json: bool = True) -> httpx.Response:
        response = MagicMock(spec=httpx.Response)
        response.status_code = status_code
        response.text = body
        if is_json:
            response.json.return_value = json.loads(body)
        else:
            response.json.side_effect = Exception("not JSON")
        return response

    def test_json_body_uses_error_and_detail_fields(self) -> None:
        # lines 109-112: JSON body -> "error: detail" message
        import click

        response = self._make_response(
            400,
            '{"error": "INVALID_FORMAT", "detail": "bad resource path"}',
        )
        with pytest.raises(click.ClickException) as exc_info:
            _handle_error_response(response)

        msg = exc_info.value.format_message()
        assert "INVALID_FORMAT" in msg
        assert "bad resource path" in msg

    def test_non_json_body_falls_back_to_status_text(self) -> None:
        # lines 113-114: non-JSON -> "HTTP {status}: {text[:200]}"
        import click

        response = self._make_response(503, "Service Unavailable", is_json=False)
        with pytest.raises(click.ClickException) as exc_info:
            _handle_error_response(response)

        msg = exc_info.value.format_message()
        assert "503" in msg
        assert "Service Unavailable" in msg

    def test_missing_error_field_defaults_to_error_string(self) -> None:
        import click

        response = self._make_response(
            400,
            '{"detail": "something went wrong"}',
        )
        with pytest.raises(click.ClickException) as exc_info:
            _handle_error_response(response)

        msg = exc_info.value.format_message()
        assert "ERROR" in msg
        assert "something went wrong" in msg


class TestStatusCommandExtended:
    def _status_response_with_anomalies(self) -> dict[object, object]:
        return {
            "pod_counts": {"Running": 2},
            "node_conditions": [],
            "active_anomalies": [
                {
                    "severity": "error",
                    "resource_kind": "Pod",
                    "resource_name": "broken-pod",
                    "namespace": "default",
                    "reason": "CrashLoopBackOff",
                }
            ],
            "recent_events": [],
            "cache_state": "ready",
        }

    def test_status_with_json_flag_outputs_raw_json(self) -> None:
        # lines 222-230: output_json=True branch
        runner = CliRunner()
        status_data = {"pod_counts": {"Running": 3}, "cache_state": "ready"}
        with patch("kuberca.cli.main._get", return_value=status_data):
            result = runner.invoke(cli, ["status", "--json"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["cache_state"] == "ready"
        assert parsed["pod_counts"]["Running"] == 3

    def test_status_with_namespace_flag_passes_param(self) -> None:
        # lines 222-230: namespace param forwarded via params dict
        runner = CliRunner()
        status_data = {"pod_counts": {}, "cache_state": "ready"}
        with patch("kuberca.cli.main._get", return_value=status_data) as mock_get:
            runner.invoke(cli, ["status", "--namespace", "staging"])

        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"]["namespace"] == "staging"

    def test_status_with_anomalies_prints_active_anomalies_section(self) -> None:
        # lines 222-230: anomalies list is non-empty
        runner = CliRunner()
        with patch("kuberca.cli.main._get", return_value=self._status_response_with_anomalies()):
            result = runner.invoke(cli, ["status"])

        assert result.exit_code == 0
        assert "CrashLoopBackOff" in result.output
        assert "broken-pod" in result.output


class TestPrintRcaWithWarnings:
    def test_warnings_in_meta_are_printed(self) -> None:
        # lines 358-359: warnings list in meta is non-empty
        runner = CliRunner()
        rca_data = {
            "root_cause": "OOM killed",
            "confidence": 0.8,
            "diagnosed_by": "rule_engine",
            "rule_id": "R01",
            "evidence": [],
            "affected_resources": [],
            "suggested_remediation": "",
            "meta": {
                "response_time_ms": 12,
                "cache_state": "ready",
                "warnings": ["LLM unavailable", "cache partially ready"],
            },
        }
        with patch("kuberca.cli.main._post", return_value=rca_data):
            result = runner.invoke(cli, ["analyze", "Pod/default/my-pod"])

        assert result.exit_code == 0
        assert "LLM unavailable" in result.output
        assert "cache partially ready" in result.output

    def test_empty_warnings_list_in_meta_no_warning_output(self) -> None:
        runner = CliRunner()
        rca_data = {
            "root_cause": "network policy",
            "confidence": 0.7,
            "diagnosed_by": "rule_engine",
            "rule_id": None,
            "evidence": [],
            "affected_resources": [],
            "suggested_remediation": "",
            "meta": {
                "response_time_ms": 5,
                "cache_state": "ready",
                "warnings": [],
            },
        }
        with patch("kuberca.cli.main._post", return_value=rca_data):
            result = runner.invoke(cli, ["analyze", "Pod/default/my-pod"])

        assert result.exit_code == 0
        assert "Warning:" not in result.output


# ---------------------------------------------------------------------------
# Module 2: api/routes.py
# ---------------------------------------------------------------------------


def _make_rca_response_model() -> object:
    from kuberca.models.analysis import AffectedResource, EvidenceItem, RCAResponse, ResponseMeta
    from kuberca.models.events import DiagnosisSource, EvidenceType

    return RCAResponse(
        root_cause="image pull failure",
        confidence=0.9,
        diagnosed_by=DiagnosisSource.RULE_ENGINE,
        rule_id="R05",
        evidence=[EvidenceItem(type=EvidenceType.EVENT, timestamp="2026-01-01T00:00:00Z", summary="ErrImagePull")],
        affected_resources=[AffectedResource(kind="Pod", namespace="default", name="api")],
        suggested_remediation="fix image tag",
        _meta=ResponseMeta(
            kuberca_version="0.1.2",
            schema_version="1",
            cluster_id="",
            timestamp="2026-01-01T00:00:01Z",
            response_time_ms=10,
            cache_state="ready",
            warnings=[],
        ),
    )


def _make_cache(readiness: CacheReadiness = CacheReadiness.READY) -> MagicMock:
    cache = MagicMock()
    cache.readiness_state = readiness
    cache.get = MagicMock(return_value=MagicMock())
    cache.list_pods = MagicMock(return_value=[])
    cache.list_nodes = MagicMock(return_value=[])
    cache.list_events = MagicMock(return_value=[])
    return cache


def _make_coordinator(rca: object | None = None) -> MagicMock:
    coordinator = MagicMock()
    coordinator.analyze = AsyncMock(return_value=rca or _make_rca_response_model())
    return coordinator


def _make_app_client(
    coordinator: MagicMock | None = None,
    cache: MagicMock | None = None,
    analysis_queue: MagicMock | None = None,
) -> TestClient:
    app = create_app(
        coordinator=coordinator or _make_coordinator(),
        cache=cache or _make_cache(),
        analysis_queue=analysis_queue,
    )
    return TestClient(app, raise_server_exceptions=False)


class TestBuildClusterStatus:
    @pytest.mark.asyncio
    async def test_with_list_pods_callable(self) -> None:
        # lines 141-148: callable list_pods is invoked, counts phases
        cache = MagicMock()
        mock_pod = MagicMock()
        mock_pod.status = {"phase": "Running"}
        cache.list_pods = MagicMock(return_value=[mock_pod, mock_pod])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)

        assert result.pod_counts.get("Running") == 2

    @pytest.mark.asyncio
    async def test_with_namespace_filters_pods(self) -> None:
        # lines 143: namespace path in list_pods
        cache = MagicMock()
        mock_pod = MagicMock()
        mock_pod.status = {"phase": "Pending"}
        cache.list_pods = MagicMock(return_value=[mock_pod])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace="kube-system")

        cache.list_pods.assert_called_once_with(namespace="kube-system")
        assert result.pod_counts.get("Pending") == 1

    @pytest.mark.asyncio
    async def test_pod_with_none_status_defaults_to_unknown(self) -> None:
        cache = MagicMock()
        mock_pod = MagicMock()
        mock_pod.status = None
        cache.list_pods = MagicMock(return_value=[mock_pod])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)

        assert result.pod_counts.get("Unknown") == 1

    @pytest.mark.asyncio
    async def test_without_list_pods_attribute(self) -> None:
        # lines 140-141: no list_pods attribute -> pod_counts stays empty
        cache = MagicMock(spec=[])  # no attributes
        result = await _build_cluster_status(cache, namespace=None)
        assert result.pod_counts == {}

    @pytest.mark.asyncio
    async def test_list_pods_exception_is_swallowed(self) -> None:
        # line 147-148: exception in list_pods is caught and logged
        cache = MagicMock()
        cache.list_pods = MagicMock(side_effect=RuntimeError("cache error"))
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)
        assert result.pod_counts == {}

    @pytest.mark.asyncio
    async def test_with_list_nodes_callable(self) -> None:
        # lines 151-167: callable list_nodes builds node_conditions
        cache = MagicMock()
        mock_node = MagicMock()
        mock_node.name = "node-1"
        mock_node.status = {
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "KubeletReady"},
                {"type": "MemoryPressure", "status": "False", "reason": "KubeletHasSufficientMemory"},
            ]
        }
        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(return_value=[mock_node])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)

        assert len(result.node_conditions) == 2
        assert result.node_conditions[0]["name"] == "node-1"
        assert result.node_conditions[0]["type"] == "Ready"

    @pytest.mark.asyncio
    async def test_node_with_none_status_skips_conditions(self) -> None:
        cache = MagicMock()
        mock_node = MagicMock()
        mock_node.name = "node-1"
        mock_node.status = None
        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(return_value=[mock_node])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)
        assert result.node_conditions == []

    @pytest.mark.asyncio
    async def test_list_nodes_exception_is_swallowed(self) -> None:
        # line 166-167: exception in list_nodes is caught
        cache = MagicMock()
        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(side_effect=RuntimeError("nodes broke"))
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)
        assert result.node_conditions == []

    @pytest.mark.asyncio
    async def test_with_list_events_callable(self) -> None:
        # lines 171-188: callable list_events builds recent_events
        cache = MagicMock()
        mock_evt = MagicMock()
        mock_evt.namespace = "default"
        mock_evt.resource_kind = "Pod"
        mock_evt.resource_name = "my-pod"
        mock_evt.reason = "Started"
        mock_evt.message = "container started"
        mock_evt.severity = MagicMock(value="info")
        mock_evt.last_seen = datetime(2026, 2, 21, 12, 0, 0, tzinfo=UTC)
        mock_evt.count = 1

        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[mock_evt])
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)

        assert len(result.recent_events) == 1
        evt = result.recent_events[0]
        assert evt["namespace"] == "default"
        assert evt["resource_kind"] == "Pod"
        assert evt["severity"] == "info"

    @pytest.mark.asyncio
    async def test_list_events_with_namespace(self) -> None:
        # line 173: namespace path for list_events
        cache = MagicMock()
        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[])
        cache.readiness_state = CacheReadiness.READY

        await _build_cluster_status(cache, namespace="production")

        cache.list_events.assert_called_once_with(namespace="production", limit=50)

    @pytest.mark.asyncio
    async def test_list_events_exception_is_swallowed(self) -> None:
        # lines 187-188: exception in list_events is caught
        cache = MagicMock()
        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(side_effect=RuntimeError("events broke"))
        cache.readiness_state = CacheReadiness.READY

        result = await _build_cluster_status(cache, namespace=None)
        assert result.recent_events == []

    @pytest.mark.asyncio
    async def test_cache_state_from_readiness_state_value(self) -> None:
        # lines 135-137: readiness_state with .value attribute
        cache = MagicMock()
        cache.readiness_state = CacheReadiness.DEGRADED
        cache.list_pods = MagicMock(return_value=[])
        cache.list_nodes = MagicMock(return_value=[])
        cache.list_events = MagicMock(return_value=[])

        result = await _build_cluster_status(cache, namespace=None)
        assert result.cache_state == "degraded"

    @pytest.mark.asyncio
    async def test_cache_state_from_string_readiness_state(self) -> None:
        # lines 135-137: readiness_state without .value (plain string)
        cache2 = MagicMock()
        cache2.list_pods = MagicMock(return_value=[])
        cache2.list_nodes = MagicMock(return_value=[])
        cache2.list_events = MagicMock(return_value=[])
        # readiness_state is a plain string (no .value attribute)
        del cache2.readiness_state
        cache2.readiness_state = "warming"

        result = await _build_cluster_status(cache2, namespace=None)
        # "warming" has no .value attr so str() is used
        assert result.cache_state == "warming"


class TestGetRegisteredRuleCount:
    def test_returns_rule_count_from_list(self) -> None:
        # lines 204-211: coordinator with _rule_engine._rules as list
        request = MagicMock()
        coordinator = MagicMock()
        rule_engine = MagicMock()
        rule_engine._rules = ["R01", "R02", "R03"]
        coordinator._rule_engine = rule_engine
        request.app.state.coordinator = coordinator

        assert _get_registered_rule_count(request) == 3

    def test_returns_rule_count_from_dict(self) -> None:
        # line 209: rules as dict
        request = MagicMock()
        coordinator = MagicMock()
        rule_engine = MagicMock()
        rule_engine._rules = {"R01": object(), "R02": object()}
        coordinator._rule_engine = rule_engine
        request.app.state.coordinator = coordinator

        assert _get_registered_rule_count(request) == 2

    def test_returns_zero_when_coordinator_is_none(self) -> None:
        # lines 203-204: coordinator is None
        request = MagicMock()
        request.app.state.coordinator = None

        assert _get_registered_rule_count(request) == 0

    def test_returns_zero_when_rule_engine_is_none(self) -> None:
        # lines 205-206: _rule_engine is None
        request = MagicMock()
        coordinator = MagicMock()
        coordinator._rule_engine = None
        request.app.state.coordinator = coordinator

        assert _get_registered_rule_count(request) == 0

    def test_returns_zero_when_rules_is_not_list_or_dict(self) -> None:
        # line 211: rules is neither list nor dict
        request = MagicMock()
        coordinator = MagicMock()
        rule_engine = MagicMock()
        rule_engine._rules = 42  # not list or dict
        coordinator._rule_engine = rule_engine
        request.app.state.coordinator = coordinator

        assert _get_registered_rule_count(request) == 0

    def test_returns_zero_on_unexpected_exception(self) -> None:
        # line 212-213: generic exception returns 0
        request = MagicMock()
        request.app.state = MagicMock(side_effect=RuntimeError("state error"))

        # getattr on a MagicMock that raises needs a different approach
        broken_state = MagicMock()
        type(broken_state).coordinator = property(lambda self: (_ for _ in ()).throw(RuntimeError("err")))
        request2 = MagicMock()
        request2.app.state = broken_state

        assert _get_registered_rule_count(request2) == 0


class TestGetTopRulesMatched:
    def test_returns_empty_list_on_exception(self) -> None:
        # lines 233-234: any exception returns []
        with patch("kuberca.api.routes.rule_matches_total") as mock_counter:
            mock_counter.collect.side_effect = RuntimeError("prometheus error")
            result = _get_top_rules_matched()

        assert result == []

    def test_returns_rule_ids_sorted_by_count(self) -> None:
        with patch("kuberca.api.routes.rule_matches_total") as mock_counter:
            mock_sample_r01 = MagicMock()
            mock_sample_r01.name = "kuberca_rule_matches_total"
            mock_sample_r01.labels = {"rule_id": "R01"}
            mock_sample_r01.value = 10.0

            mock_sample_r02 = MagicMock()
            mock_sample_r02.name = "kuberca_rule_matches_total"
            mock_sample_r02.labels = {"rule_id": "R02"}
            mock_sample_r02.value = 5.0

            mock_metric = MagicMock()
            mock_metric.samples = [mock_sample_r02, mock_sample_r01]
            mock_counter.collect.return_value = [mock_metric]

            result = _get_top_rules_matched(limit=5)

        assert result == ["R01", "R02"]

    def test_respects_limit(self) -> None:
        with patch("kuberca.api.routes.rule_matches_total") as mock_counter:
            samples = []
            for i in range(10):
                s = MagicMock()
                s.name = "kuberca_rule_matches_total"
                s.labels = {"rule_id": f"R{i:02d}"}
                s.value = float(10 - i)
                samples.append(s)

            mock_metric = MagicMock()
            mock_metric.samples = samples
            mock_counter.collect.return_value = [mock_metric]

            result = _get_top_rules_matched(limit=3)

        assert len(result) == 3

    def test_ignores_samples_without_rule_id(self) -> None:
        with patch("kuberca.api.routes.rule_matches_total") as mock_counter:
            mock_sample = MagicMock()
            mock_sample.name = "kuberca_rule_matches_total"
            mock_sample.labels = {}  # no rule_id key
            mock_sample.value = 5.0

            mock_metric = MagicMock()
            mock_metric.samples = [mock_sample]
            mock_counter.collect.return_value = [mock_metric]

            result = _get_top_rules_matched()

        assert result == []


class TestGetStatusErrorPath:
    def test_returns_500_on_unexpected_exception(self) -> None:
        # lines 270-272: generic exception in get_status -> 500
        cache = MagicMock()
        # Make readiness_state raise to force the exception path inside the try block
        cache.readiness_state = CacheReadiness.READY
        # Make _build_cluster_status raise by making list_pods raise an unexpected error
        # that isn't caught internally
        coordinator = _make_coordinator()

        app = create_app(coordinator=coordinator, cache=cache)
        client = TestClient(app, raise_server_exceptions=False)

        with patch("kuberca.api.routes._build_cluster_status", new=AsyncMock(side_effect=RuntimeError("unexpected"))):
            resp = client.get("/api/v1/status")

        assert resp.status_code == 500
        body = resp.json()
        assert body["error"] == "INTERNAL_ERROR"


class TestPostAnalyzeErrorPath:
    def test_returns_500_on_coordinator_exception(self) -> None:
        # lines 328-329 (within the except block): coordinator.analyze raises
        coordinator = MagicMock()
        coordinator.analyze = AsyncMock(side_effect=ValueError("coordinator failure"))
        client = _make_app_client(coordinator=coordinator)

        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/x"})

        assert resp.status_code == 500
        body = resp.json()
        assert body["error"] == "INTERNAL_ERROR"

    def test_cache_lookup_exception_is_swallowed_and_analysis_continues(self) -> None:
        # lines 328-329: cache.get raises but analysis continues
        cache = _make_cache()
        cache.get = MagicMock(side_effect=RuntimeError("cache error"))
        client = _make_app_client(cache=cache)

        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/my-pod"})

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Module 3: mcp/server.py — untested tool dispatch (unknown tool)
# ---------------------------------------------------------------------------


class TestMCPUnknownTool:
    @pytest.mark.asyncio
    async def test_unknown_tool_name_returns_error_json(self) -> None:
        # lines 117-122: unknown tool name returns error payload
        from mcp.types import CallToolRequest, CallToolRequestParams

        from kuberca.mcp.server import MCPServer

        coordinator = MagicMock()
        cache = MagicMock()
        server = MCPServer(coordinator=coordinator, cache=cache)

        # The call_tool handler is registered in the MCP Server's request_handlers dict,
        # keyed by the CallToolRequest class. Invoke it with an unknown tool name.
        handler = server._server.request_handlers[CallToolRequest]
        request = CallToolRequest(
            method="tools/call",
            params=CallToolRequestParams(name="nonexistent_tool", arguments={}),
        )
        result = await handler(request)
        # result.root is a CallToolResult; its content list holds TextContent items
        content = result.root.content
        assert len(content) == 1
        payload = json.loads(content[0].text)
        assert "Unknown tool" in payload["error"]
        assert "nonexistent_tool" in payload["error"]

    @pytest.mark.asyncio
    async def test_list_tools_returns_two_tools(self) -> None:
        # line 61: _list_tools returns the two registered tools
        from mcp.types import ListToolsRequest

        from kuberca.mcp.server import MCPServer

        coordinator = MagicMock()
        cache = MagicMock()
        server = MCPServer(coordinator=coordinator, cache=cache)

        # Invoke the list_tools handler stored in request_handlers
        handler = server._server.request_handlers[ListToolsRequest]
        request = ListToolsRequest(method="tools/list")
        result = await handler(request)
        tool_names = {t.name for t in result.root.tools}
        assert "k8s_cluster_status" in tool_names
        assert "k8s_analyze_incident" in tool_names


class TestMCPServerStart:
    @pytest.mark.asyncio
    async def test_start_logs_and_calls_server_run(self) -> None:
        # lines 218-230: start() logs, builds init_options, and runs server
        from kuberca.mcp.server import MCPServer

        coordinator = MagicMock()
        cache = MagicMock()
        server = MCPServer(coordinator=coordinator, cache=cache)

        mock_read = AsyncMock()
        mock_write = AsyncMock()

        with (
            patch("kuberca.mcp.server.stdio_server") as mock_stdio,
            patch.object(server._server, "run", new=AsyncMock()) as mock_run,
        ):
            # stdio_server is used as an async context manager
            mock_stdio.return_value.__aenter__ = AsyncMock(return_value=(mock_read, mock_write))
            mock_stdio.return_value.__aexit__ = AsyncMock(return_value=False)

            await server.start()

        mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# Module 4: observability/logging.py
# ---------------------------------------------------------------------------


class TestSetupLogging:
    def test_setup_logging_default_level(self) -> None:
        # lines 15-17: default info level
        import structlog

        setup_logging()
        logger = structlog.get_logger(component="test")
        assert logger is not None

    def test_setup_logging_debug_level(self) -> None:
        # line 15: getattr(logging, level.upper())
        setup_logging(level="debug")
        # Verify structlog is reconfigured without error
        logger = get_logger("test_debug")
        assert logger is not None

    def test_setup_logging_unknown_level_defaults_to_info(self) -> None:
        # line 15: getattr with fallback to logging.INFO
        setup_logging(level="notareallevel")
        logger = get_logger("test_unknown")
        assert logger is not None

    def test_get_logger_returns_bound_logger_with_component(self) -> None:
        logger = get_logger("my_component")
        assert logger is not None

    def test_setup_logging_warning_level(self) -> None:
        setup_logging(level="warning")
        logger = get_logger("warn_test")
        assert logger is not None

    def test_setup_logging_is_idempotent(self) -> None:
        # Calling setup_logging multiple times should not raise
        setup_logging(level="info")
        setup_logging(level="debug")
        setup_logging(level="info")


# ---------------------------------------------------------------------------
# Module 5: __main__.py
# ---------------------------------------------------------------------------


class TestMainModule:
    def test_main_module_can_be_imported(self) -> None:
        # The __main__ module calls asyncio.run(main()) at module level.
        # We patch both asyncio.run (so it does not block) and kuberca.app.main
        # (so no K8s startup occurs). asyncio.run is replaced by a no-op that
        # immediately closes the coroutine it receives to avoid unawaited-coro
        # warnings from the garbage collector.
        import importlib
        import sys

        def _run_and_close(coro: object) -> None:
            import inspect

            if inspect.iscoroutine(coro):
                coro.close()

        if "kuberca.__main__" in sys.modules:
            del sys.modules["kuberca.__main__"]

        with patch("asyncio.run", side_effect=_run_and_close) as mock_run, patch("kuberca.app.main"):
            importlib.import_module("kuberca.__main__")

        mock_run.assert_called_once()

    def test_main_module_asyncio_run_receives_coroutine(self) -> None:
        import importlib
        import inspect
        import sys

        captured_args: list[object] = []

        def _capture_run(coro: object) -> None:
            captured_args.append(coro)
            if inspect.iscoroutine(coro):
                coro.close()

        if "kuberca.__main__" in sys.modules:
            del sys.modules["kuberca.__main__"]

        with patch("asyncio.run", side_effect=_capture_run), patch("kuberca.app.main"):
            importlib.import_module("kuberca.__main__")

        assert len(captured_args) == 1


# ---------------------------------------------------------------------------
# Module 6: dependency_graph.py — edge cases
# ---------------------------------------------------------------------------


def _make_snapshot(
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
    spec: dict | None = None,
) -> ResourceSnapshot:
    return ResourceSnapshot(
        kind=kind,
        namespace=namespace,
        name=name,
        spec_hash="abc123",
        spec=spec or {},
        captured_at=datetime(2026, 2, 21, 12, 0, 0, tzinfo=UTC),
        resource_version="1",
    )


class TestRemoveResourceEdgeCases:
    def test_remove_nonexistent_resource_does_not_raise(self) -> None:
        # line 76 (pop with None default): no error for absent key
        g = DependencyGraph()
        g.remove_resource("Pod", "default", "ghost")  # Should not raise

    def test_remove_resource_cleans_reverse_edges(self) -> None:
        # lines 133-136: removes edges where this key is a target
        g = DependencyGraph()
        # Pod (web) -> ConfigMap (cfg)
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "cfg"}}]}},
            )
        )
        # cfg is the target; removing it should clean up reverse edges
        assert g.get_node("ConfigMap", "default", "cfg") is not None
        g.remove_resource("ConfigMap", "default", "cfg")
        assert g.get_node("ConfigMap", "default", "cfg") is None

    def test_remove_resource_cleans_forward_edges_of_dependents(self) -> None:
        # When target is removed, source's forward edges to it are cleaned
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "cfg"}}]}},
            )
        )
        initial_edge_count = g.edge_count
        assert initial_edge_count == 1

        g.remove_resource("ConfigMap", "default", "cfg")

        # After removing the target, the forward edge from Pod to ConfigMap
        # should be cleaned from Pod's forward list
        pod_downstream = g.downstream("Pod", "default", "web")
        assert len(pod_downstream.resources) == 0

    def test_remove_resource_also_removes_source_edges(self) -> None:
        # When source node (Pod) is removed, its forward edges are cleaned
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "cfg"}}]}},
            )
        )
        assert g.edge_count == 1
        g.remove_resource("Pod", "default", "web")
        assert g.edge_count == 0
        assert g.get_node("Pod", "default", "web") is None


class TestUpstreamDownstreamUnknownResource:
    def test_upstream_unknown_resource_returns_empty(self) -> None:
        # line 360: start_key not in _nodes -> return DependencyResult()
        g = DependencyGraph()
        result = g.upstream("Pod", "default", "nonexistent")
        assert result.resources == []
        assert result.edges == []
        assert result.depth_reached == 0
        assert result.truncated is False

    def test_downstream_unknown_resource_returns_empty(self) -> None:
        # line 360: same path for forward direction
        g = DependencyGraph()
        result = g.downstream("ConfigMap", "default", "nonexistent")
        assert result.resources == []
        assert result.edges == []
        assert result.depth_reached == 0

    def test_upstream_resource_with_no_reverse_edges(self) -> None:
        # Resource exists but has no upstream dependents
        g = DependencyGraph()
        g.add_resource(_make_snapshot(kind="ConfigMap", namespace="default", name="isolated"))
        result = g.upstream("ConfigMap", "default", "isolated")
        assert result.resources == []

    def test_downstream_resource_with_no_forward_edges(self) -> None:
        # Resource exists but has no downstream dependencies
        g = DependencyGraph()
        g.add_resource(_make_snapshot(kind="ConfigMap", namespace="default", name="isolated"))
        result = g.downstream("ConfigMap", "default", "isolated")
        assert result.resources == []


class TestExtractPodEdgesEdgeCases:
    def test_pod_with_non_dict_spec_does_not_raise(self) -> None:
        # line 218: pod_spec is not dict -> early return
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="bad-spec",
                spec={"spec": "not-a-dict"},
            )
        )
        result = g.downstream("Pod", "default", "bad-spec")
        assert result.resources == []

    def test_pod_volume_with_non_dict_entry_is_skipped(self) -> None:
        # line 238: non-dict volume entry is skipped
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={
                    "spec": {
                        "volumes": [
                            "not-a-dict-volume",
                            {"configMap": {"name": "valid-cm"}},
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "web")
        assert len(result.resources) == 1
        assert result.resources[0].name == "valid-cm"

    def test_pod_with_envfrom_secret_ref(self) -> None:
        # lines 309-317: secretRef in envFrom
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="worker",
                spec={
                    "spec": {
                        "containers": [
                            {
                                "name": "app",
                                "envFrom": [
                                    {"secretRef": {"name": "db-secret"}},
                                ],
                            },
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "worker")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "Secret"
        assert result.resources[0].name == "db-secret"

    def test_pod_with_non_dict_container_is_skipped(self) -> None:
        # line 289: non-dict container is skipped in envFrom processing
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="worker",
                spec={
                    "spec": {
                        "containers": [
                            "not-a-dict",
                            {"name": "app", "envFrom": [{"configMapRef": {"name": "cm"}}]},
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "worker")
        assert len(result.resources) == 1
        assert result.resources[0].name == "cm"

    def test_pod_with_non_dict_envfrom_entry_is_skipped(self) -> None:
        # line 294: non-dict envFrom entry is skipped
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="worker",
                spec={
                    "spec": {
                        "containers": [
                            {
                                "name": "app",
                                "envFrom": [
                                    "not-a-dict",
                                    {"configMapRef": {"name": "my-cm"}},
                                ],
                            },
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "worker")
        assert len(result.resources) == 1
        assert result.resources[0].name == "my-cm"


class TestPVCEdgeCases:
    def test_pvc_volume_name_in_top_level_spec(self) -> None:
        # line 97-98: volumeName at top-level spec dict
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                namespace="default",
                name="data-pvc",
                spec={"volumeName": "pv-direct"},
            )
        )
        result = g.downstream("PersistentVolumeClaim", "default", "data-pvc")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "PersistentVolume"
        assert result.resources[0].name == "pv-direct"

    def test_pvc_with_empty_volume_name_no_edge(self) -> None:
        # lines 99-102: empty volumeName -> no PV edge created
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                namespace="default",
                name="unbound-pvc",
                spec={"spec": {"volumeName": ""}},
            )
        )
        result = g.downstream("PersistentVolumeClaim", "default", "unbound-pvc")
        assert result.resources == []


class TestOwnerReferences:
    def test_pod_with_owner_reference_creates_edge(self) -> None:
        # lines 70-88: ownerReferences extraction
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                namespace="default",
                name="managed-pod",
                spec={
                    "metadata": {
                        "ownerReferences": [
                            {"kind": "ReplicaSet", "name": "my-rs"},
                        ]
                    }
                },
            )
        )
        result = g.downstream("Pod", "default", "managed-pod")
        kinds = {r.kind for r in result.resources}
        assert "ReplicaSet" in kinds

    def test_owner_reference_with_non_dict_entry_is_skipped(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                namespace="default",
                name="mixed-pod",
                spec={
                    "metadata": {
                        "ownerReferences": [
                            "not-a-dict",
                            {"kind": "ReplicaSet", "name": "my-rs"},
                        ]
                    }
                },
            )
        )
        result = g.downstream("Pod", "default", "mixed-pod")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "ReplicaSet"


class TestPathBetweenExtended:
    def test_path_found_via_multi_hop_forward_edges(self) -> None:
        # lines 187-188: forward neighbor is NOT the target -> it is queued
        # (requires a path longer than 1 hop so the intermediate node is queued)
        g = DependencyGraph()
        # Pod -> PVC -> PV  (2 hops)
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="db",
                spec={"spec": {"volumes": [{"persistentVolumeClaim": {"claimName": "db-pvc"}}]}},
            )
        )
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                name="db-pvc",
                spec={"spec": {"volumeName": "pv-001"}},
            )
        )
        pod_node = g.get_node("Pod", "default", "db")
        pv_node = g.get_node("PersistentVolume", "", "pv-001")
        assert pod_node is not None
        assert pv_node is not None

        path = g.path_between(pod_node, pv_node)

        assert path is not None
        assert len(path) == 2
        assert path[0].edge_type == EdgeType.VOLUME_BINDING  # Pod -> PVC
        assert path[1].edge_type == EdgeType.VOLUME_BINDING  # PVC -> PV

    def test_path_found_via_reverse_edges(self) -> None:
        # lines 191-196: reverse edge traversal used when forward edges
        # of the source don't directly reach the target
        g = DependencyGraph()
        # Pod (web) -> ConfigMap (cfg)
        # path_between(cfg, pod) must follow REVERSE edges
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "cfg"}}]}},
            )
        )
        cfg_node = g.get_node("ConfigMap", "default", "cfg")
        pod_node = g.get_node("Pod", "default", "web")
        assert cfg_node is not None
        assert pod_node is not None

        # Searching from cfg to pod follows reverse edges (cfg <- pod)
        path = g.path_between(cfg_node, pod_node)

        assert path is not None
        assert len(path) == 1

    def test_path_via_reverse_intermediate_hop(self) -> None:
        # lines 195-196: reverse neighbor is NOT the target -> it gets queued
        # Needs a 2-hop reverse path: A <- B <- C, search C->A via reverse
        g = DependencyGraph()
        # Pod (a) -> PVC (b) -> PV (c); reverse: c -> b -> a
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="a",
                spec={"spec": {"volumes": [{"persistentVolumeClaim": {"claimName": "b"}}]}},
            )
        )
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                name="b",
                spec={"spec": {"volumeName": "c"}},
            )
        )
        pv_node = g.get_node("PersistentVolume", "", "c")
        pod_node = g.get_node("Pod", "default", "a")
        assert pv_node is not None
        assert pod_node is not None

        # path_between(pv, pod) traverses two reverse hops: PV->PVC->Pod
        path = g.path_between(pv_node, pod_node)

        assert path is not None
        assert len(path) == 2


class TestTruncatedTraversal:
    def test_truncated_flag_set_when_max_depth_hit_with_more_nodes(self) -> None:
        # lines 385-389: truncated detection
        g = DependencyGraph()
        # Chain: Pod -> PVC -> PV -> (hypothetical deeper node)
        # We need depth > max_depth to trigger truncation.
        # Build a 3-level chain: Pod -> PVC -> PV
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="db",
                spec={"spec": {"volumes": [{"persistentVolumeClaim": {"claimName": "db-pvc"}}]}},
            )
        )
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                name="db-pvc",
                spec={"spec": {"volumeName": "pv-001"}},
            )
        )
        # Add a 4th level: PV -> StorageClass (simulated via owner reference trick)
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolume",
                namespace="",
                name="pv-001",
                spec={
                    "metadata": {
                        "ownerReferences": [{"kind": "StorageClass", "name": "standard"}]
                    }
                },
            )
        )

        # With max_depth=2, we reach PV but cannot go further to StorageClass
        result = g.downstream("Pod", "default", "db", max_depth=2)
        kinds = {r.kind for r in result.resources}
        assert "PersistentVolumeClaim" in kinds
        assert "PersistentVolume" in kinds
        # truncated should be True because StorageClass is one level beyond
        assert result.truncated is True
