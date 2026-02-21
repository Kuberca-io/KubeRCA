"""Tests for uncovered coordinator.py paths — helpers and edge cases.

Covers lines: 184, 283, 306-308, 425-428, 529, 617, 662-743,
749-751, 759, 763-767, 774, 777-781, 800-802, 812-814, 866, 870, 874,
877-879, 891-898, 911-922, 945, 968-991, 1019, 1040, 1049-1051.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from kuberca.analyst.coordinator import (
    AnalystCoordinator,
    ResourceFormatError,
    _llm_result_to_rca_response,
    _parse_resource,
)
from kuberca.graph.models import DependencyResult, EdgeType, GraphEdge, GraphNode
from kuberca.llm.analyzer import LLMResult
from kuberca.models.analysis import (
    AffectedResource,
    EvaluationMeta,
    QualityCheckResult,
    RuleResult,
)
from kuberca.models.events import DiagnosisSource, EventRecord, EventSource, Severity
from kuberca.models.resources import CachedResourceView, CacheReadiness, FieldChange

# ---------------------------------------------------------------------------
# Shared test fixtures / factories
# ---------------------------------------------------------------------------


def _ts() -> datetime:
    return datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)


def _make_event(
    reason: str = "OOMKilled",
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
) -> EventRecord:
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test",
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message="test event",
        namespace=namespace,
        resource_kind=kind,
        resource_name=name,
        first_seen=_ts(),
        last_seen=_ts(),
    )


def _make_cached_view(
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
    status: dict | None = None,
    spec: dict | None = None,
) -> CachedResourceView:
    return CachedResourceView(
        kind=kind,
        namespace=namespace,
        name=name,
        resource_version="1",
        labels={},
        annotations={},
        spec=spec or {},
        status=status or {},
        last_updated=_ts(),
    )


def _make_coordinator(
    cache_readiness: CacheReadiness = CacheReadiness.READY,
    events: list[EventRecord] | None = None,
    rule_result: RuleResult | None = None,
    llm_analyzer: object | None = None,
    cache_get_return: CachedResourceView | None = None,
    dependency_graph: object | None = None,
) -> AnalystCoordinator:
    cache = MagicMock()
    cache.readiness.return_value = cache_readiness
    cache.get.return_value = cache_get_return

    ledger = MagicMock()
    ledger.diff.return_value = []

    if events is None:
        events = [_make_event()]
    event_buffer = MagicMock()
    event_buffer.get_events.return_value = events

    eval_meta = EvaluationMeta(rules_evaluated=1, rules_matched=1 if rule_result else 0)
    rule_engine = MagicMock()
    rule_engine.evaluate.return_value = (rule_result, eval_meta)

    config = MagicMock()
    config.cluster_id = "test-cluster"
    config.ollama = MagicMock()
    config.ollama.endpoint = "http://localhost:11434"

    return AnalystCoordinator(
        rule_engine=rule_engine,
        llm_analyzer=llm_analyzer,
        cache=cache,
        ledger=ledger,
        event_buffer=event_buffer,
        config=config,
        dependency_graph=dependency_graph,
    )


def _make_llm_result(
    success: bool = True,
    confidence: float = 0.55,
    root_cause: str = "Image pull failed",
    citations: list[str] | None = None,
) -> LLMResult:
    return LLMResult(
        success=success,
        root_cause=root_cause,
        confidence=confidence,
        evidence_citations=citations or ["2026-02-18T12:00:00.000Z evidence one"],
        affected_resources=[AffectedResource("Pod", "default", "my-pod")],
        suggested_remediation="Fix the image tag",
        causal_chain="The tag was wrong.",
    )


# ---------------------------------------------------------------------------
# TestParseResourceLongStrings — line 184
# ---------------------------------------------------------------------------


class TestParseResourceLongStrings:
    def test_namespace_too_long_raises(self) -> None:
        long_ns = "a" * 254
        with pytest.raises(ResourceFormatError) as exc_info:
            _parse_resource(f"pod/{long_ns}/my-pod")
        assert exc_info.value.code == "INVALID_RESOURCE_FORMAT"
        assert "253" in str(exc_info.value)

    def test_name_too_long_raises(self) -> None:
        long_name = "b" * 254
        with pytest.raises(ResourceFormatError) as exc_info:
            _parse_resource(f"pod/default/{long_name}")
        assert exc_info.value.code == "INVALID_RESOURCE_FORMAT"
        assert "253" in str(exc_info.value)

    def test_exactly_253_chars_allowed(self) -> None:
        valid_ns = "a" * 253
        valid_name = "b" * 253
        kind, ns, name = _parse_resource(f"pod/{valid_ns}/{valid_name}")
        assert kind == "Pod"
        assert ns == valid_ns
        assert name == valid_name


# ---------------------------------------------------------------------------
# TestLlmResultToRcaResponse — lines 283, 306-308
# ---------------------------------------------------------------------------


class TestLlmResultToRcaResponse:
    def test_degraded_applies_confidence_penalty(self) -> None:
        llm_result = _make_llm_result(confidence=0.60)
        eval_meta = EvaluationMeta()
        response = _llm_result_to_rca_response(
            llm_result=llm_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        # 0.60 - 0.10 = 0.50, capped at 0.70
        assert response.confidence == pytest.approx(0.50, abs=0.01)
        assert response.diagnosed_by == DiagnosisSource.LLM

    def test_degraded_penalty_floors_at_zero(self) -> None:
        llm_result = _make_llm_result(confidence=0.05)
        eval_meta = EvaluationMeta()
        response = _llm_result_to_rca_response(
            llm_result=llm_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        assert response.confidence == pytest.approx(0.0, abs=0.01)

    def test_degraded_adds_warning(self) -> None:
        llm_result = _make_llm_result(confidence=0.55)
        eval_meta = EvaluationMeta()
        response = _llm_result_to_rca_response(
            llm_result=llm_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        assert response._meta is not None
        assert any("degraded" in w.lower() for w in response._meta.warnings)

    def test_partially_ready_adds_warning_no_penalty(self) -> None:
        # LLM path: PARTIALLY_READY adds a warning but does NOT reduce confidence
        llm_result = _make_llm_result(confidence=0.60)
        eval_meta = EvaluationMeta()
        response = _llm_result_to_rca_response(
            llm_result=llm_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        # No penalty in the LLM path for PARTIALLY_READY, capped at 0.70
        assert response.confidence == pytest.approx(0.60, abs=0.01)
        assert response._meta is not None
        assert any("partially ready" in w.lower() for w in response._meta.warnings)

    def test_ready_no_warning_no_penalty(self) -> None:
        llm_result = _make_llm_result(confidence=0.60)
        eval_meta = EvaluationMeta()
        response = _llm_result_to_rca_response(
            llm_result=llm_result,
            cache_readiness=CacheReadiness.READY,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        assert response.confidence == pytest.approx(0.60, abs=0.01)
        assert response._meta is not None
        assert not any("degraded" in w.lower() or "partially" in w.lower() for w in response._meta.warnings)

    def test_confidence_capped_at_0_70(self) -> None:
        llm_result = _make_llm_result(confidence=0.90)
        eval_meta = EvaluationMeta()
        response = _llm_result_to_rca_response(
            llm_result=llm_result,
            cache_readiness=CacheReadiness.READY,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        assert response.confidence == pytest.approx(0.70, abs=0.01)


# ---------------------------------------------------------------------------
# TestInvalidTimeWindowInAnalyze — lines 425-428
# ---------------------------------------------------------------------------


class TestInvalidTimeWindowInAnalyze:
    @pytest.mark.asyncio
    async def test_invalid_time_window_returns_inconclusive(self) -> None:
        coordinator = _make_coordinator()
        response = await coordinator.analyze("pod/default/my-pod", time_window="99h")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        assert "time_window" in response.root_cause.lower() or "24h" in response.root_cause

    @pytest.mark.asyncio
    async def test_invalid_time_window_format_returns_inconclusive(self) -> None:
        coordinator = _make_coordinator()
        response = await coordinator.analyze("pod/default/my-pod", time_window="abc")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE

    @pytest.mark.asyncio
    async def test_invalid_time_window_meta_has_warning(self) -> None:
        coordinator = _make_coordinator()
        response = await coordinator.analyze("pod/default/my-pod", time_window="99h")
        assert response._meta is not None
        assert len(response._meta.warnings) > 0


# ---------------------------------------------------------------------------
# TestSynthesiseStatusEvents — lines 662-743
# ---------------------------------------------------------------------------


class TestSynthesiseStatusEvents:
    def _make_coordinator_for_synth(self, status: dict) -> AnalystCoordinator:
        cached = _make_cached_view(status=status)
        return _make_coordinator(cache_get_return=cached)

    def test_non_pod_kind_returns_empty(self) -> None:
        coordinator = _make_coordinator()
        result = coordinator._synthesise_status_events("Deployment", "default", "my-deploy")
        assert result == []

    def test_cache_returns_none_returns_empty(self) -> None:
        coordinator = _make_coordinator(cache_get_return=None)
        result = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert result == []

    def test_cache_raises_returns_empty(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.get.side_effect = RuntimeError("cache failure")
        result = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert result == []

    def test_oomkilled_terminated_state_generates_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {
                        "terminated": {
                            "reason": "OOMKilled",
                            "message": "container was OOMKilled",
                        }
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "OOMKilled"
        assert events[0].severity == Severity.CRITICAL
        assert events[0].source == EventSource.POD_PHASE

    def test_oomkilling_terminated_state_generates_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {
                        "terminated": {"reason": "OOMKilling"},
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "OOMKilling"

    def test_crashloopbackoff_waiting_state_generates_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {
                        "waiting": {
                            "reason": "CrashLoopBackOff",
                            "message": "back-off restarting failed container",
                        }
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "CrashLoopBackOff"
        assert events[0].severity == Severity.ERROR
        assert events[0].source == EventSource.CORE_EVENT

    def test_imagepullbackoff_waiting_state_generates_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {
                        "waiting": {"reason": "ImagePullBackOff"},
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "ImagePullBackOff"

    def test_errimagepull_waiting_state_generates_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {
                        "waiting": {"reason": "ErrImagePull"},
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "ErrImagePull"

    def test_laststate_terminated_oomkilled_generates_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {},
                    "lastState": {
                        "terminated": {
                            "reason": "OOMKilled",
                            "message": "killed by OOM",
                        }
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "OOMKilled"

    def test_init_container_crashloopbackoff_generates_event(self) -> None:
        status = {
            "initContainerStatuses": [
                {
                    "name": "init-app",
                    "state": {
                        "waiting": {
                            "reason": "CrashLoopBackOff",
                            "message": "init container crashing",
                        }
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "CrashLoopBackOff"

    def test_init_container_imagepullbackoff_generates_event(self) -> None:
        status = {
            "initContainerStatuses": [
                {
                    "name": "init-app",
                    "state": {
                        "waiting": {"reason": "ImagePullBackOff"},
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert len(events) == 1
        assert events[0].reason == "ImagePullBackOff"

    def test_non_dict_container_status_entries_skipped(self) -> None:
        status = {
            "containerStatuses": [
                "not-a-dict",
                42,
                None,
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert events == []

    def test_non_dict_init_container_status_entries_skipped(self) -> None:
        status = {
            "initContainerStatuses": [
                "not-a-dict",
                None,
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert events == []

    def test_container_statuses_not_a_list_returns_empty(self) -> None:
        # If containerStatuses is not a list the code should treat it as empty
        status = {"containerStatuses": "bad-value"}
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert events == []

    def test_unrecognised_reason_produces_no_event(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {
                        "terminated": {"reason": "Completed"},
                        "waiting": {"reason": "ContainerCreating"},
                    },
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert events == []

    def test_event_namespace_and_name_match_args(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {"terminated": {"reason": "OOMKilled"}},
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "prod", "web-pod")
        assert events[0].namespace == "prod"
        assert events[0].resource_name == "web-pod"
        assert events[0].resource_kind == "Pod"

    def test_terminated_message_fallback_when_absent(self) -> None:
        status = {
            "containerStatuses": [
                {
                    "name": "app",
                    "state": {"terminated": {"reason": "OOMKilled"}},
                }
            ]
        }
        coordinator = self._make_coordinator_for_synth(status)
        events = coordinator._synthesise_status_events("Pod", "default", "my-pod")
        assert "OOMKilled" in events[0].message


# ---------------------------------------------------------------------------
# TestWarmingWarnings — lines 749-767
# ---------------------------------------------------------------------------


class TestWarmingWarnings:
    def test_with_warming_kinds_method_returns_per_kind_warnings(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.warming_kinds = MagicMock(return_value=["Pod", "Node"])
        warnings = coordinator._warming_warnings()
        assert len(warnings) == 1
        assert "Pod" in warnings[0]
        assert "Node" in warnings[0]

    def test_with_warming_kinds_returns_empty_list_gives_no_warnings(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.warming_kinds = MagicMock(return_value=[])
        warnings = coordinator._warming_warnings()
        assert warnings == []

    def test_without_warming_kinds_method_returns_fallback(self) -> None:
        coordinator = _make_coordinator()
        # Remove the attribute so getattr returns None → AttributeError path
        del coordinator._cache.warming_kinds
        warnings = coordinator._warming_warnings()
        assert len(warnings) == 1
        assert "warming" in warnings[0].lower()

    def test_warming_kinds_raises_generic_exception_returns_fallback(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.warming_kinds = MagicMock(side_effect=RuntimeError("unexpected"))
        warnings = coordinator._warming_warnings()
        assert len(warnings) == 1
        assert "warming" in warnings[0].lower()


# ---------------------------------------------------------------------------
# TestDegradedWarnings — lines 774-781
# ---------------------------------------------------------------------------


class TestDegradedWarnings:
    def test_with_staleness_warnings_method_returns_list(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.staleness_warnings = MagicMock(return_value=["Pods stale", "Nodes stale"])
        warnings = coordinator._degraded_warnings()
        assert warnings == ["Pods stale", "Nodes stale"]

    def test_staleness_warnings_returns_non_list_gives_empty(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.staleness_warnings = MagicMock(return_value="not-a-list")
        warnings = coordinator._degraded_warnings()
        assert warnings == []

    def test_without_staleness_warnings_method_returns_fallback(self) -> None:
        coordinator = _make_coordinator()
        del coordinator._cache.staleness_warnings
        warnings = coordinator._degraded_warnings()
        assert len(warnings) == 1
        assert "degraded" in warnings[0].lower()

    def test_staleness_warnings_raises_generic_exception_returns_fallback(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.staleness_warnings = MagicMock(side_effect=RuntimeError("boom"))
        warnings = coordinator._degraded_warnings()
        assert len(warnings) == 1
        assert "degraded" in warnings[0].lower()


# ---------------------------------------------------------------------------
# TestGetEventsErrorPath — lines 800-802
# ---------------------------------------------------------------------------


class TestGetEventsErrorPath:
    def test_event_buffer_raises_returns_empty_list(self) -> None:
        coordinator = _make_coordinator()
        coordinator._event_buffer.get_events.side_effect = RuntimeError("buffer failure")
        result = coordinator._get_events("Pod", "default", "my-pod")
        assert result == []


# ---------------------------------------------------------------------------
# TestEvaluateRulesErrorPath — lines 812-814
# ---------------------------------------------------------------------------


class TestEvaluateRulesErrorPath:
    def test_rule_engine_raises_returns_none_and_empty_meta(self) -> None:
        coordinator = _make_coordinator()
        coordinator._rule_engine.evaluate.side_effect = RuntimeError("engine failure")
        event = _make_event()
        result, meta = coordinator._evaluate_rules(event)
        assert result is None
        assert meta.rules_evaluated == 0
        assert meta.rules_matched == 0
        assert meta.duration_ms == 0.0


# ---------------------------------------------------------------------------
# TestGetLedgerChanges — lines 866, 870, 874, 877-879
# ---------------------------------------------------------------------------


class TestGetLedgerChanges:
    def _make_field_change(self) -> FieldChange:
        return FieldChange(
            field_path="spec.replicas",
            old_value="1",
            new_value="3",
            changed_at=_ts(),
        )

    def test_time_window_hours_calls_ledger_with_timedelta_hours(self) -> None:
        coordinator = _make_coordinator()
        coordinator._ledger.diff.return_value = [self._make_field_change()]
        result = coordinator._get_ledger_changes("Pod", "default", "my-pod", "2h")
        assert len(result) == 1
        call_kwargs = coordinator._ledger.diff.call_args
        since: timedelta = call_kwargs.kwargs["since"]
        assert since == timedelta(hours=2)

    def test_time_window_minutes_calls_ledger_with_timedelta_minutes(self) -> None:
        coordinator = _make_coordinator()
        coordinator._ledger.diff.return_value = []
        coordinator._get_ledger_changes("Pod", "default", "my-pod", "30m")
        call_kwargs = coordinator._ledger.diff.call_args
        since: timedelta = call_kwargs.kwargs["since"]
        assert since == timedelta(minutes=30)

    def test_time_window_days_calls_ledger_with_timedelta_days(self) -> None:
        coordinator = _make_coordinator()
        coordinator._ledger.diff.return_value = []
        coordinator._get_ledger_changes("Pod", "default", "my-pod", "1d")
        call_kwargs = coordinator._ledger.diff.call_args
        since: timedelta = call_kwargs.kwargs["since"]
        assert since == timedelta(days=1)

    def test_invalid_time_window_returns_empty(self) -> None:
        coordinator = _make_coordinator()
        result = coordinator._get_ledger_changes("Pod", "default", "my-pod", "bad-window")
        assert result == []
        coordinator._ledger.diff.assert_not_called()

    def test_ledger_raises_returns_empty(self) -> None:
        coordinator = _make_coordinator()
        coordinator._ledger.diff.side_effect = RuntimeError("ledger failure")
        result = coordinator._get_ledger_changes("Pod", "default", "my-pod", "2h")
        assert result == []


# ---------------------------------------------------------------------------
# TestGetContainerStatuses — lines 891-898
# ---------------------------------------------------------------------------


class TestGetContainerStatuses:
    def test_returns_container_status_dicts(self) -> None:
        container_status = {"name": "app", "ready": True, "restartCount": 0}
        cached = _make_cached_view(status={"containerStatuses": [container_status]})
        coordinator = _make_coordinator(cache_get_return=cached)
        result = coordinator._get_container_statuses("default", "my-pod")
        assert result == [container_status]

    def test_returns_empty_when_resource_not_found(self) -> None:
        coordinator = _make_coordinator(cache_get_return=None)
        result = coordinator._get_container_statuses("default", "my-pod")
        assert result == []

    def test_returns_empty_on_exception(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.get.side_effect = RuntimeError("cache error")
        result = coordinator._get_container_statuses("default", "my-pod")
        assert result == []

    def test_non_list_container_statuses_returns_empty(self) -> None:
        cached = _make_cached_view(status={"containerStatuses": "not-a-list"})
        coordinator = _make_coordinator(cache_get_return=cached)
        result = coordinator._get_container_statuses("default", "my-pod")
        assert result == []

    def test_filters_out_non_dict_entries(self) -> None:
        cached = _make_cached_view(
            status={
                "containerStatuses": [
                    {"name": "valid"},
                    "string-entry",
                    None,
                    {"name": "also-valid"},
                ]
            }
        )
        coordinator = _make_coordinator(cache_get_return=cached)
        result = coordinator._get_container_statuses("default", "my-pod")
        assert len(result) == 2
        assert result[0]["name"] == "valid"
        assert result[1]["name"] == "also-valid"

    def test_empty_container_statuses_list_returns_empty(self) -> None:
        cached = _make_cached_view(status={"containerStatuses": []})
        coordinator = _make_coordinator(cache_get_return=cached)
        result = coordinator._get_container_statuses("default", "my-pod")
        assert result == []


# ---------------------------------------------------------------------------
# TestGetResourceSpecs — lines 911-922
# ---------------------------------------------------------------------------


class TestGetResourceSpecs:
    def test_returns_resource_spec_dict(self) -> None:
        cached = _make_cached_view(
            kind="Pod",
            namespace="default",
            name="my-pod",
            spec={"containers": [{"name": "app", "image": "nginx:latest"}]},
            status={"phase": "Running"},
        )
        coordinator = _make_coordinator(cache_get_return=cached)
        result = coordinator._get_resource_specs("Pod", "default", "my-pod")
        assert len(result) == 1
        spec_dict = result[0]
        assert spec_dict["kind"] == "Pod"
        assert spec_dict["namespace"] == "default"
        assert spec_dict["name"] == "my-pod"
        assert "containers" in spec_dict["spec"]
        assert spec_dict["status"]["phase"] == "Running"

    def test_returns_empty_when_resource_not_found(self) -> None:
        coordinator = _make_coordinator(cache_get_return=None)
        result = coordinator._get_resource_specs("Pod", "default", "missing-pod")
        assert result == []

    def test_returns_empty_on_exception(self) -> None:
        coordinator = _make_coordinator()
        coordinator._cache.get.side_effect = RuntimeError("cache error")
        result = coordinator._get_resource_specs("Pod", "default", "my-pod")
        assert result == []


# ---------------------------------------------------------------------------
# TestBuildStateContext — lines 945, 968-991
# ---------------------------------------------------------------------------


def _make_graph_node(kind: str, namespace: str, name: str) -> GraphNode:
    return GraphNode(kind=kind, namespace=namespace, name=name)


def _make_graph_edge(
    src: GraphNode,
    tgt: GraphNode,
    edge_type: EdgeType = EdgeType.OWNER_REFERENCE,
) -> GraphEdge:
    return GraphEdge(
        source=src,
        target=tgt,
        edge_type=edge_type,
        source_field="metadata.ownerReferences",
    )


class TestBuildStateContext:
    def test_no_dependency_graph_returns_empty(self) -> None:
        coordinator = _make_coordinator(dependency_graph=None)
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert result == []

    def test_downstream_resources_included(self) -> None:
        dep_graph = MagicMock()
        pvc_node = _make_graph_node("PersistentVolumeClaim", "default", "my-pvc")
        svc_node = _make_graph_node("Service", "default", "my-svc")
        pod_node = _make_graph_node("Pod", "default", "my-pod")

        downstream_result = DependencyResult(
            resources=[pod_node, pvc_node, svc_node],  # pod_node is self — should be skipped
            edges=[],
        )
        upstream_result = DependencyResult(resources=[], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        cached_pvc = _make_cached_view(kind="PersistentVolumeClaim", status={"phase": "Bound"})
        cache = MagicMock()
        cache.readiness.return_value = CacheReadiness.READY
        cache.get.return_value = cached_pvc

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=cached_pvc)
        coordinator._cache = cache
        result = coordinator._build_state_context("Pod", "default", "my-pod")

        # pod_node (self) should be skipped; pvc and svc should be included
        assert len(result) == 2
        kinds = {e.kind for e in result}
        assert "PersistentVolumeClaim" in kinds
        assert "Service" in kinds

    def test_cache_get_returns_none_sets_not_found(self) -> None:
        dep_graph = MagicMock()
        pvc_node = _make_graph_node("PersistentVolumeClaim", "default", "missing-pvc")
        downstream_result = DependencyResult(resources=[pvc_node], edges=[])
        upstream_result = DependencyResult(resources=[], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=None)
        result = coordinator._build_state_context("Pod", "default", "my-pod")

        assert len(result) == 1
        assert result[0].exists is False
        assert result[0].status_summary == "<not found>"

    def test_duplicate_resources_from_upstream_are_filtered(self) -> None:
        dep_graph = MagicMock()
        pvc_node = _make_graph_node("PersistentVolumeClaim", "default", "shared-pvc")
        downstream_result = DependencyResult(resources=[pvc_node], edges=[])
        # Same pvc_node also in upstream — should be deduplicated
        upstream_result = DependencyResult(resources=[pvc_node], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=None)
        result = coordinator._build_state_context("Pod", "default", "my-pod")

        matching = [e for e in result if e.kind == "PersistentVolumeClaim" and e.name == "shared-pvc"]
        assert len(matching) == 1

    def test_capped_at_10_entries(self) -> None:
        dep_graph = MagicMock()
        nodes = [_make_graph_node("Service", "default", f"svc-{i}") for i in range(15)]
        downstream_result = DependencyResult(resources=nodes, edges=[])
        upstream_result = DependencyResult(resources=[], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=None)
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert len(result) <= 10

    def test_exception_returns_partial_results(self) -> None:
        dep_graph = MagicMock()
        dep_graph.downstream.side_effect = RuntimeError("graph failure")
        dep_graph.upstream.return_value = DependencyResult(resources=[], edges=[])

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=None)
        # Should not raise, returns whatever was built before the exception
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert isinstance(result, list)

    def test_upstream_self_is_skipped(self) -> None:
        dep_graph = MagicMock()
        pod_node = _make_graph_node("Pod", "default", "my-pod")
        downstream_result = DependencyResult(resources=[], edges=[])
        upstream_result = DependencyResult(resources=[pod_node], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=None)
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert result == []

    def test_upstream_fallback_relationship_string_when_no_edge_match(self) -> None:
        dep_graph = MagicMock()
        deploy_node = _make_graph_node("Deployment", "default", "my-deploy")
        downstream_result = DependencyResult(resources=[], edges=[])
        upstream_result = DependencyResult(resources=[deploy_node], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=None)
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert len(result) == 1
        # Fallback: "<Kind> depends on <kind>/<name>"
        assert "depends on" in result[0].relationship

    def test_resource_with_phase_uses_phase_as_status_summary(self) -> None:
        dep_graph = MagicMock()
        pvc_node = _make_graph_node("PersistentVolumeClaim", "default", "my-pvc")
        downstream_result = DependencyResult(resources=[pvc_node], edges=[])
        upstream_result = DependencyResult(resources=[], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        cached = _make_cached_view(kind="PersistentVolumeClaim", status={"phase": "Bound"})
        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=cached)
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert len(result) == 1
        assert result[0].status_summary == "Bound"
        assert result[0].exists is True

    def test_resource_without_phase_uses_active_as_status_summary(self) -> None:
        dep_graph = MagicMock()
        svc_node = _make_graph_node("Service", "default", "my-svc")
        downstream_result = DependencyResult(resources=[svc_node], edges=[])
        upstream_result = DependencyResult(resources=[], edges=[])
        dep_graph.downstream.return_value = downstream_result
        dep_graph.upstream.return_value = upstream_result

        cached = _make_cached_view(kind="Service", status={})
        coordinator = _make_coordinator(dependency_graph=dep_graph, cache_get_return=cached)
        result = coordinator._build_state_context("Pod", "default", "my-pod")
        assert result[0].status_summary == "Active"


# ---------------------------------------------------------------------------
# TestEdgeRelationship — line 1019
# ---------------------------------------------------------------------------


class TestEdgeRelationship:
    def test_returns_empty_string_when_no_matching_edge(self) -> None:
        src = _make_graph_node("Pod", "default", "my-pod")
        tgt = _make_graph_node("Service", "default", "my-svc")
        unrelated_edge = _make_graph_edge(
            _make_graph_node("Deployment", "default", "my-deploy"),
            _make_graph_node("ReplicaSet", "default", "my-rs"),
        )
        result = AnalystCoordinator._edge_relationship(
            edges=[unrelated_edge],
            src_kind=src.kind,
            src_ns=src.namespace,
            src_name=src.name,
            target=tgt,
        )
        assert result == ""

    def test_returns_empty_string_for_empty_edge_list(self) -> None:
        tgt = _make_graph_node("Service", "default", "my-svc")
        result = AnalystCoordinator._edge_relationship(
            edges=[],
            src_kind="Pod",
            src_ns="default",
            src_name="my-pod",
            target=tgt,
        )
        assert result == ""

    def test_returns_relationship_string_for_matching_edge(self) -> None:
        src_node = _make_graph_node("Pod", "default", "my-pod")
        tgt_node = _make_graph_node("Service", "default", "my-svc")
        edge = _make_graph_edge(src_node, tgt_node, EdgeType.SERVICE_SELECTOR)
        result = AnalystCoordinator._edge_relationship(
            edges=[edge],
            src_kind="Pod",
            src_ns="default",
            src_name="my-pod",
            target=tgt_node,
        )
        assert "service_selector" in result
        assert "Pod" in result
        assert "Service" in result

    def test_returns_relationship_string_for_reversed_edge(self) -> None:
        src_node = _make_graph_node("Service", "default", "my-svc")
        tgt_node = _make_graph_node("Pod", "default", "my-pod")
        edge = _make_graph_edge(src_node, tgt_node, EdgeType.SERVICE_SELECTOR)
        # Query from Pod's perspective with reversed edge
        result = AnalystCoordinator._edge_relationship(
            edges=[edge],
            src_kind="Pod",
            src_ns="default",
            src_name="my-pod",
            target=src_node,  # asking about the Service
        )
        assert "service_selector" in result


# ---------------------------------------------------------------------------
# TestComputeBlastRadius — lines 1040, 1049-1051
# ---------------------------------------------------------------------------


class TestComputeBlastRadius:
    def test_no_dependency_graph_returns_none(self) -> None:
        coordinator = _make_coordinator(dependency_graph=None)
        result = coordinator._compute_blast_radius("Pod", "default", "my-pod")
        assert result is None

    def test_skips_self_in_upstream_results(self) -> None:
        dep_graph = MagicMock()
        self_node = _make_graph_node("Pod", "default", "my-pod")
        upstream_result = DependencyResult(resources=[self_node], edges=[])
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph)
        result = coordinator._compute_blast_radius("Pod", "default", "my-pod")
        # Only self in results — should return None (no resources excluding self)
        assert result is None

    def test_returns_none_when_no_upstream_resources_excluding_self(self) -> None:
        dep_graph = MagicMock()
        upstream_result = DependencyResult(resources=[], edges=[])
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph)
        result = coordinator._compute_blast_radius("Pod", "default", "my-pod")
        assert result is None

    def test_returns_affected_resources_for_upstream(self) -> None:
        dep_graph = MagicMock()
        deploy_node = _make_graph_node("Deployment", "default", "my-deploy")
        upstream_result = DependencyResult(resources=[deploy_node], edges=[])
        dep_graph.upstream.return_value = upstream_result

        coordinator = _make_coordinator(dependency_graph=dep_graph)
        result = coordinator._compute_blast_radius("Pod", "default", "my-pod")
        assert result is not None
        assert len(result) == 1
        assert result[0].kind == "Deployment"
        assert result[0].name == "my-deploy"

    def test_returns_none_on_exception(self) -> None:
        dep_graph = MagicMock()
        dep_graph.upstream.side_effect = RuntimeError("graph failure")

        coordinator = _make_coordinator(dependency_graph=dep_graph)
        result = coordinator._compute_blast_radius("Pod", "default", "my-pod")
        assert result is None


# ---------------------------------------------------------------------------
# TestLlmFailurePath — line 617
# ---------------------------------------------------------------------------


class TestLlmFailurePath:
    @pytest.mark.asyncio
    async def test_llm_failure_falls_through_to_inconclusive(self) -> None:
        llm_result = _make_llm_result(success=False, confidence=0.0, root_cause="LLM timeout")
        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=llm_result)

        coordinator = _make_coordinator(rule_result=None, llm_analyzer=llm_analyzer)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        # LLM should have been called but returned failure
        llm_analyzer.analyze.assert_called_once()


# ---------------------------------------------------------------------------
# TestLlmEscalationWithQualityCheck — line 529
# ---------------------------------------------------------------------------


class TestLlmEscalationWithQualityCheck:
    @pytest.mark.asyncio
    async def test_partially_ready_cache_adds_warning_to_llm_path(self) -> None:
        llm_result = _make_llm_result(
            success=True,
            confidence=0.60,
            root_cause="OOMKilled due to memory pressure",
        )
        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=llm_result)
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=True, failures=[]),
        )

        coordinator = _make_coordinator(
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            rule_result=None,
            llm_analyzer=llm_analyzer,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.LLM
        assert response._meta is not None
        assert any("partially ready" in w.lower() for w in response._meta.warnings)

    @pytest.mark.asyncio
    async def test_quality_check_passing_returns_llm_response(self) -> None:
        llm_result = _make_llm_result(
            success=True,
            confidence=0.55,
            root_cause="OOMKilled due to memory pressure",
        )
        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=llm_result)
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=True, failures=[]),
        )

        coordinator = _make_coordinator(
            cache_readiness=CacheReadiness.READY,
            rule_result=None,
            llm_analyzer=llm_analyzer,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.LLM
        # quality_check must have been called once (loop exited on first pass)
        llm_analyzer.quality_check.assert_called_once()
