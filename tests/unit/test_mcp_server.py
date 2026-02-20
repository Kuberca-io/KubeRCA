"""Unit tests for kuberca.mcp.server — MCPServer and _rca_response_to_dict."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kuberca.mcp.server import MCPServer, _rca_response_to_dict
from kuberca.models.analysis import (
    AffectedResource,
    EvidenceItem,
    RCAResponse,
    ResponseMeta,
)
from kuberca.models.events import DiagnosisSource, EvidenceType

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_coordinator(rca: RCAResponse | None = None) -> MagicMock:
    coordinator = MagicMock()
    coordinator.analyze = AsyncMock(return_value=rca or _make_rca_response())
    return coordinator


def _make_rca_response(
    *,
    root_cause: str = "Pod OOM killed due to memory limit",
    confidence: float = 0.85,
    diagnosed_by: DiagnosisSource = DiagnosisSource.RULE_ENGINE,
    rule_id: str | None = "R01_oom_killed",
    with_meta: bool = False,
) -> RCAResponse:
    meta = (
        ResponseMeta(
            kuberca_version="0.1.0",
            schema_version="1",
            cluster_id="test-cluster",
            timestamp="2026-02-20T10:00:00Z",
            response_time_ms=37,
            cache_state="ready",
            warnings=[],
        )
        if with_meta
        else None
    )
    return RCAResponse(
        root_cause=root_cause,
        confidence=confidence,
        diagnosed_by=diagnosed_by,
        rule_id=rule_id,
        evidence=[
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp="2026-02-20T10:00:00Z",
                summary="OOMKilled event observed",
                debug_context=None,
            )
        ],
        affected_resources=[
            AffectedResource(kind="Pod", namespace="default", name="my-pod"),
        ],
        suggested_remediation="Increase memory limit",
        _meta=meta,
    )


def _make_mcp_server(
    coordinator: MagicMock | None = None,
    cache: MagicMock | None = None,
) -> MCPServer:
    return MCPServer(
        coordinator=coordinator or _make_coordinator(),
        cache=cache or MagicMock(),
    )


# ---------------------------------------------------------------------------
# TestMCPServerInit
# ---------------------------------------------------------------------------


class TestMCPServerInit:
    def test_server_creation(self) -> None:
        """MCPServer can be instantiated with coordinator and cache mocks."""
        coordinator = MagicMock()
        cache = MagicMock()

        server = MCPServer(coordinator=coordinator, cache=cache)

        assert server is not None
        assert server._coordinator is coordinator
        assert server._cache is cache

    def test_handler_registration(self) -> None:
        """_register_handlers is called during __init__, wiring list_tools and call_tool."""
        coordinator = MagicMock()
        cache = MagicMock()

        with patch.object(MCPServer, "_register_handlers") as mock_register:
            MCPServer(coordinator=coordinator, cache=cache)
            mock_register.assert_called_once()

    def test_internal_mcp_server_created(self) -> None:
        """The underlying MCP Server object is created on init."""
        server = _make_mcp_server()
        # _server must be a real MCP Server instance, not a mock
        assert server._server is not None


# ---------------------------------------------------------------------------
# TestHandleClusterStatus
# ---------------------------------------------------------------------------


class TestHandleClusterStatus:
    @pytest.mark.asyncio
    async def test_success(self) -> None:
        """Returns a JSON TextContent payload when _build_cluster_status succeeds."""
        from kuberca.api.schemas import ClusterStatus

        mock_status = ClusterStatus(
            pod_counts={"Running": 3, "Pending": 1},
            node_conditions=[{"name": "node-1", "type": "Ready", "status": "True", "reason": ""}],
            active_anomalies=[],
            recent_events=[],
            cache_state="ready",
        )

        server = _make_mcp_server()
        with patch("kuberca.api.routes._build_cluster_status", new=AsyncMock(return_value=mock_status)):
            result = await server._handle_cluster_status({})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["pod_counts"] == {"Running": 3, "Pending": 1}
        assert payload["cache_state"] == "ready"
        assert "node_conditions" in payload
        assert "active_anomalies" in payload
        assert "recent_events" in payload

    @pytest.mark.asyncio
    async def test_success_with_namespace_arg(self) -> None:
        """namespace from args is forwarded to _build_cluster_status."""
        from kuberca.api.schemas import ClusterStatus

        mock_status = ClusterStatus(
            pod_counts={"Running": 1},
            node_conditions=[],
            active_anomalies=[],
            recent_events=[],
            cache_state="ready",
        )

        server = _make_mcp_server()
        captured: list[str | None] = []

        async def _fake_build(cache: object, namespace: str | None) -> ClusterStatus:
            captured.append(namespace)
            return mock_status

        with patch("kuberca.api.routes._build_cluster_status", new=_fake_build):
            await server._handle_cluster_status({"namespace": "production"})

        assert captured == ["production"]

    @pytest.mark.asyncio
    async def test_error(self) -> None:
        """Returns isError=True payload when _build_cluster_status raises an exception."""
        server = _make_mcp_server()
        with patch(
            "kuberca.api.routes._build_cluster_status",
            new=AsyncMock(side_effect=RuntimeError("cache exploded")),
        ):
            result = await server._handle_cluster_status({})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INTERNAL_ERROR"
        assert "cache exploded" in payload["detail"]

    @pytest.mark.asyncio
    async def test_no_namespace_arg_passes_none(self) -> None:
        """When namespace is absent from args, None is forwarded to _build_cluster_status."""
        from kuberca.api.schemas import ClusterStatus

        mock_status = ClusterStatus(
            pod_counts={},
            node_conditions=[],
            active_anomalies=[],
            recent_events=[],
            cache_state="warming",
        )

        server = _make_mcp_server()
        captured: list[str | None] = []

        async def _fake_build(cache: object, namespace: str | None) -> ClusterStatus:
            captured.append(namespace)
            return mock_status

        with patch("kuberca.api.routes._build_cluster_status", new=_fake_build):
            await server._handle_cluster_status({})

        assert captured == [None]


# ---------------------------------------------------------------------------
# TestHandleAnalyzeIncident
# ---------------------------------------------------------------------------


class TestHandleAnalyzeIncident:
    @pytest.mark.asyncio
    async def test_valid_resource(self) -> None:
        """Returns RCA JSON payload for a well-formed Kind/namespace/name resource."""
        rca = _make_rca_response()
        coordinator = _make_coordinator(rca=rca)
        server = _make_mcp_server(coordinator=coordinator)

        result = await server._handle_analyze_incident({"resource": "Pod/default/my-pod"})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["root_cause"] == "Pod OOM killed due to memory limit"
        assert payload["confidence"] == pytest.approx(0.85)
        assert payload["diagnosed_by"] == "rule_engine"
        assert payload["rule_id"] == "R01_oom_killed"

    @pytest.mark.asyncio
    async def test_empty_resource(self) -> None:
        """Returns INVALID_RESOURCE_FORMAT error when resource is an empty string."""
        server = _make_mcp_server()

        result = await server._handle_analyze_incident({"resource": ""})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INVALID_RESOURCE_FORMAT"

    @pytest.mark.asyncio
    async def test_malformed_resource_two_parts(self) -> None:
        """Returns INVALID_RESOURCE_FORMAT error when resource has only 2 slash-separated parts."""
        server = _make_mcp_server()

        result = await server._handle_analyze_incident({"resource": "Pod/default"})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INVALID_RESOURCE_FORMAT"
        assert "Pod/default" in payload["detail"]

    @pytest.mark.asyncio
    async def test_malformed_resource_one_part(self) -> None:
        """Returns INVALID_RESOURCE_FORMAT error for a bare string with no slashes."""
        server = _make_mcp_server()

        result = await server._handle_analyze_incident({"resource": "my-pod"})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INVALID_RESOURCE_FORMAT"

    @pytest.mark.asyncio
    async def test_missing_resource(self) -> None:
        """Returns INVALID_RESOURCE_FORMAT error when 'resource' key is absent from args."""
        server = _make_mcp_server()

        result = await server._handle_analyze_incident({})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INVALID_RESOURCE_FORMAT"

    @pytest.mark.asyncio
    async def test_coordinator_exception(self) -> None:
        """Returns INTERNAL_ERROR payload when coordinator.analyze raises an exception."""
        coordinator = MagicMock()
        coordinator.analyze = AsyncMock(side_effect=RuntimeError("coordinator exploded"))
        server = _make_mcp_server(coordinator=coordinator)

        result = await server._handle_analyze_incident({"resource": "Pod/default/my-pod"})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INTERNAL_ERROR"
        assert "coordinator exploded" in payload["detail"]

    @pytest.mark.asyncio
    async def test_time_window_passed(self) -> None:
        """time_window arg is forwarded to coordinator.analyze."""
        rca = _make_rca_response()
        coordinator = _make_coordinator(rca=rca)
        server = _make_mcp_server(coordinator=coordinator)

        await server._handle_analyze_incident({"resource": "Pod/default/my-pod", "time_window": "1h"})

        coordinator.analyze.assert_called_once_with(
            resource="Pod/default/my-pod",
            time_window="1h",
        )

    @pytest.mark.asyncio
    async def test_default_time_window_is_2h(self) -> None:
        """When time_window is absent, coordinator.analyze receives '2h' as default."""
        rca = _make_rca_response()
        coordinator = _make_coordinator(rca=rca)
        server = _make_mcp_server(coordinator=coordinator)

        await server._handle_analyze_incident({"resource": "Pod/default/my-pod"})

        coordinator.analyze.assert_called_once_with(
            resource="Pod/default/my-pod",
            time_window="2h",
        )

    @pytest.mark.asyncio
    async def test_resource_forwarded_correctly_to_coordinator(self) -> None:
        """The reconstructed Kind/namespace/name string is passed to coordinator.analyze."""
        rca = _make_rca_response()
        coordinator = _make_coordinator(rca=rca)
        server = _make_mcp_server(coordinator=coordinator)

        await server._handle_analyze_incident({"resource": "Deployment/production/api-server"})

        coordinator.analyze.assert_called_once_with(
            resource="Deployment/production/api-server",
            time_window="2h",
        )

    @pytest.mark.asyncio
    async def test_resource_with_empty_part_is_invalid(self) -> None:
        """Returns INVALID_RESOURCE_FORMAT when one segment is empty (e.g. 'Pod//my-pod')."""
        server = _make_mcp_server()

        result = await server._handle_analyze_incident({"resource": "Pod//my-pod"})

        assert len(result) == 1
        payload = json.loads(result[0].text)
        assert payload["isError"] is True
        assert payload["error"] == "INVALID_RESOURCE_FORMAT"


# ---------------------------------------------------------------------------
# TestRcaResponseToDict
# ---------------------------------------------------------------------------


class TestRcaResponseToDict:
    def test_basic_serialization(self) -> None:
        """All top-level fields of RCAResponse are present in the output dict."""
        rca = _make_rca_response()

        result = _rca_response_to_dict(rca)

        assert result["root_cause"] == "Pod OOM killed due to memory limit"
        assert result["confidence"] == pytest.approx(0.85)
        assert result["rule_id"] == "R01_oom_killed"
        assert result["suggested_remediation"] == "Increase memory limit"

    def test_evidence_serialized(self) -> None:
        """Evidence items are serialized with correct keys and values."""
        rca = _make_rca_response()

        result = _rca_response_to_dict(rca)

        assert len(result["evidence"]) == 1
        ev = result["evidence"][0]
        assert ev["type"] == "event"
        assert ev["timestamp"] == "2026-02-20T10:00:00Z"
        assert ev["summary"] == "OOMKilled event observed"
        assert ev["debug_context"] is None

    def test_affected_resources_serialized(self) -> None:
        """Affected resources are serialized with kind, namespace and name."""
        rca = _make_rca_response()

        result = _rca_response_to_dict(rca)

        assert len(result["affected_resources"]) == 1
        ar = result["affected_resources"][0]
        assert ar["kind"] == "Pod"
        assert ar["namespace"] == "default"
        assert ar["name"] == "my-pod"

    def test_with_meta(self) -> None:
        """_meta key is present in the result when ResponseMeta is set."""
        rca = _make_rca_response(with_meta=True)

        result = _rca_response_to_dict(rca)

        assert "_meta" in result
        meta = result["_meta"]
        assert meta["kuberca_version"] == "0.1.0"
        assert meta["schema_version"] == "1"
        assert meta["cluster_id"] == "test-cluster"
        assert meta["timestamp"] == "2026-02-20T10:00:00Z"
        assert meta["response_time_ms"] == 37
        assert meta["cache_state"] == "ready"
        assert meta["warnings"] == []

    def test_without_meta(self) -> None:
        """_meta key is absent from the result when _meta is None."""
        rca = _make_rca_response(with_meta=False)

        result = _rca_response_to_dict(rca)

        assert "_meta" not in result

    def test_evidence_type_serialized_as_string_value(self) -> None:
        """EvidenceType enum values are serialized as their string representation."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            evidence=[
                EvidenceItem(
                    type=EvidenceType.EVENT,
                    timestamp="2026-02-20T10:00:00Z",
                    summary="event summary",
                )
            ],
        )

        result = _rca_response_to_dict(rca)

        assert result["evidence"][0]["type"] == "event"
        assert result["evidence"][0]["type"] == EvidenceType.EVENT.value

    def test_evidence_change_type_serialized(self) -> None:
        """EvidenceType.CHANGE is serialized as 'change'."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            evidence=[
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp="2026-02-20T10:00:00Z",
                    summary="field changed",
                )
            ],
        )

        result = _rca_response_to_dict(rca)

        assert result["evidence"][0]["type"] == "change"

    def test_diagnosed_by_serialized_as_string_value(self) -> None:
        """DiagnosisSource enum values are serialized as their string representation."""
        rca = RCAResponse(
            root_cause="rule engine diagnosis",
            confidence=0.75,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
        )

        result = _rca_response_to_dict(rca)

        assert result["diagnosed_by"] == "rule_engine"
        assert result["diagnosed_by"] == DiagnosisSource.RULE_ENGINE.value

    def test_diagnosed_by_llm_serialized(self) -> None:
        """DiagnosisSource.LLM is serialized as 'llm'."""
        rca = RCAResponse(
            root_cause="llm diagnosis",
            confidence=0.60,
            diagnosed_by=DiagnosisSource.LLM,
        )

        result = _rca_response_to_dict(rca)

        assert result["diagnosed_by"] == "llm"

    def test_diagnosed_by_inconclusive_serialized(self) -> None:
        """DiagnosisSource.INCONCLUSIVE is serialized as 'inconclusive'."""
        rca = RCAResponse(
            root_cause="no conclusion",
            confidence=0.0,
            diagnosed_by=DiagnosisSource.INCONCLUSIVE,
        )

        result = _rca_response_to_dict(rca)

        assert result["diagnosed_by"] == "inconclusive"

    def test_result_is_json_serializable(self) -> None:
        """The returned dict can be round-tripped through json.dumps / json.loads."""
        rca = _make_rca_response(with_meta=True)

        result = _rca_response_to_dict(rca)

        serialized = json.dumps(result)
        deserialized = json.loads(serialized)
        assert deserialized["root_cause"] == rca.root_cause
        assert deserialized["diagnosed_by"] == "rule_engine"

    def test_empty_evidence_list(self) -> None:
        """An RCAResponse with no evidence produces an empty evidence list in the dict."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.INCONCLUSIVE,
        )

        result = _rca_response_to_dict(rca)

        assert result["evidence"] == []

    def test_empty_affected_resources(self) -> None:
        """An RCAResponse with no affected resources produces an empty list in the dict."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.INCONCLUSIVE,
        )

        result = _rca_response_to_dict(rca)

        assert result["affected_resources"] == []

    def test_evidence_debug_context_preserved(self) -> None:
        """debug_context is included in evidence items when set."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            evidence=[
                EvidenceItem(
                    type=EvidenceType.EVENT,
                    timestamp="2026-02-20T10:00:00Z",
                    summary="event with context",
                    debug_context={"event_reason": "OOMKilled", "exit_code": "137"},
                )
            ],
        )

        result = _rca_response_to_dict(rca)

        assert result["evidence"][0]["debug_context"] == {
            "event_reason": "OOMKilled",
            "exit_code": "137",
        }

    def test_multiple_evidence_items(self) -> None:
        """All evidence items are serialized when more than one is present."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.9,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            evidence=[
                EvidenceItem(EvidenceType.EVENT, "2026-02-20T09:00:00Z", "first event"),
                EvidenceItem(EvidenceType.CHANGE, "2026-02-20T10:00:00Z", "field change"),
            ],
        )

        result = _rca_response_to_dict(rca)

        assert len(result["evidence"]) == 2
        assert result["evidence"][0]["summary"] == "first event"
        assert result["evidence"][1]["type"] == "change"

    def test_multiple_affected_resources(self) -> None:
        """All affected resources are serialized when more than one is present."""
        rca = RCAResponse(
            root_cause="cascade failure",
            confidence=0.88,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            affected_resources=[
                AffectedResource(kind="Pod", namespace="default", name="pod-a"),
                AffectedResource(kind="Service", namespace="default", name="svc-b"),
            ],
        )

        result = _rca_response_to_dict(rca)

        assert len(result["affected_resources"]) == 2
        assert result["affected_resources"][0]["kind"] == "Pod"
        assert result["affected_resources"][1]["kind"] == "Service"

    def test_rule_id_none_preserved(self) -> None:
        """rule_id=None is preserved in the output dict as None."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.LLM,
            rule_id=None,
        )

        result = _rca_response_to_dict(rca)

        assert result["rule_id"] is None

    def test_meta_warnings_list_preserved(self) -> None:
        """warnings list inside _meta is preserved correctly."""
        rca = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            _meta=ResponseMeta(
                kuberca_version="0.1.0",
                warnings=["LLM unavailable", "cache degraded"],
            ),
        )

        result = _rca_response_to_dict(rca)

        assert result["_meta"]["warnings"] == ["LLM unavailable", "cache degraded"]
