"""Tests for kuberca.analyst.coordinator — AnalystCoordinator decision flow."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from kuberca.analyst.coordinator import (
    KIND_ALIASES,
    AnalystCoordinator,
    ResourceFormatError,
    TimeWindowFormatError,
    _parse_resource,
    _validate_time_window,
)
from kuberca.models.analysis import (
    AffectedResource,
    EvaluationMeta,
    EvidenceItem,
    RuleResult,
)
from kuberca.models.events import DiagnosisSource, EventRecord, EventSource, EvidenceType, Severity
from kuberca.models.resources import CacheReadiness


def _make_event(
    reason: str = "OOMKilled",
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
    count: int = 1,
) -> EventRecord:
    ts = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
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
        first_seen=ts,
        last_seen=ts,
        count=count,
    )


def _make_rule_result(rule_id: str = "R01_oom_killed", confidence: float = 0.75) -> RuleResult:
    return RuleResult(
        rule_id=rule_id,
        root_cause="Pod OOM killed due to memory limit",
        confidence=confidence,
        evidence=[
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp="2026-02-18T12:00:00.000Z",
                summary="OOMKilled event",
                debug_context=None,
            )
        ],
        affected_resources=[AffectedResource(kind="Pod", namespace="default", name="my-pod")],
        suggested_remediation="Increase memory limit",
    )


def _make_coordinator(
    cache_readiness: CacheReadiness = CacheReadiness.READY,
    events: list[EventRecord] | None = None,
    rule_result: RuleResult | None = None,
    llm_analyzer: object | None = None,
) -> AnalystCoordinator:
    cache = MagicMock()
    cache.readiness.return_value = cache_readiness
    cache.get.return_value = None

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
    )


class TestParseResource:
    def test_valid_lowercase_kind(self) -> None:
        kind, namespace, name = _parse_resource("pod/default/my-pod")
        assert kind == "Pod"
        assert namespace == "default"
        assert name == "my-pod"

    def test_valid_alias_deploy(self) -> None:
        kind, ns, name = _parse_resource("deploy/kube-system/coredns")
        assert kind == "Deployment"

    def test_valid_sts_alias(self) -> None:
        kind, ns, name = _parse_resource("sts/prod/my-db")
        assert kind == "StatefulSet"

    def test_unknown_kind_raises(self) -> None:
        with pytest.raises(ResourceFormatError) as exc_info:
            _parse_resource("widget/default/my-widget")
        assert exc_info.value.code == "UNKNOWN_RESOURCE_KIND"

    def test_wrong_format_raises(self) -> None:
        with pytest.raises(ResourceFormatError) as exc_info:
            _parse_resource("pod/default")
        assert exc_info.value.code == "INVALID_RESOURCE_FORMAT"

    def test_empty_namespace_raises(self) -> None:
        with pytest.raises(ResourceFormatError) as exc_info:
            _parse_resource("pod//my-pod")
        assert exc_info.value.code == "INVALID_RESOURCE_FORMAT"

    def test_hpa_alias(self) -> None:
        kind, ns, name = _parse_resource("hpa/default/my-hpa")
        assert kind == "HorizontalPodAutoscaler"


class TestValidateTimeWindow:
    def test_valid_hours(self) -> None:
        _validate_time_window("2h")  # should not raise

    def test_valid_minutes(self) -> None:
        _validate_time_window("30m")  # should not raise

    def test_valid_days(self) -> None:
        _validate_time_window("1d")  # should not raise

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(TimeWindowFormatError):
            _validate_time_window("abc")

    def test_exceeds_max_raises(self) -> None:
        with pytest.raises(TimeWindowFormatError):
            _validate_time_window("48h")


class TestKindAliases:
    def test_all_canonical_kinds_present(self) -> None:
        canonical_kinds = set(KIND_ALIASES.values())
        assert "Pod" in canonical_kinds
        assert "Deployment" in canonical_kinds
        assert "StatefulSet" in canonical_kinds
        assert "DaemonSet" in canonical_kinds
        assert "Service" in canonical_kinds
        assert "Node" in canonical_kinds

    def test_case_insensitive_lookup(self) -> None:
        assert KIND_ALIASES.get("POD".lower()) == "Pod"
        assert KIND_ALIASES.get("DEPLOY".lower()) == "Deployment"


class TestAnalystCoordinatorDecisionFlow:
    @pytest.mark.asyncio
    async def test_warming_cache_returns_inconclusive(self) -> None:
        coordinator = _make_coordinator(cache_readiness=CacheReadiness.WARMING)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        assert "initializing" in response.root_cause.lower()
        assert response._meta is not None
        assert response._meta.cache_state == "warming"

    @pytest.mark.asyncio
    async def test_no_events_returns_inconclusive(self) -> None:
        coordinator = _make_coordinator(events=[])
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        assert "no recent events" in response.root_cause.lower()

    @pytest.mark.asyncio
    async def test_rule_match_returns_rule_engine_diagnosis(self) -> None:
        rule_result = _make_rule_result("R01_oom_killed", confidence=0.75)
        coordinator = _make_coordinator(rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE
        assert response.rule_id == "R01_oom_killed"
        assert response.confidence == pytest.approx(0.75, abs=0.01)
        assert response.root_cause == "Pod OOM killed due to memory limit"

    @pytest.mark.asyncio
    async def test_rule_match_confidence_penalized_for_degraded(self) -> None:
        rule_result = _make_rule_result("R01_oom_killed", confidence=0.75)
        coordinator = _make_coordinator(
            cache_readiness=CacheReadiness.DEGRADED,
            rule_result=rule_result,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        # Penalty: -0.10 for DEGRADED
        assert response.confidence == pytest.approx(0.65, abs=0.01)

    @pytest.mark.asyncio
    async def test_rule_match_confidence_penalized_for_partially_ready(self) -> None:
        rule_result = _make_rule_result("R01_oom_killed", confidence=0.75)
        coordinator = _make_coordinator(
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            rule_result=rule_result,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        # Penalty: -0.15 for PARTIALLY_READY
        assert response.confidence == pytest.approx(0.60, abs=0.01)

    @pytest.mark.asyncio
    async def test_degraded_cache_suppresses_llm(self) -> None:
        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        coordinator = _make_coordinator(
            cache_readiness=CacheReadiness.DEGRADED,
            rule_result=None,
            llm_analyzer=llm_analyzer,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        # LLM should NOT have been called
        llm_analyzer.analyze.assert_not_called()
        assert response._meta is not None
        assert any("suppressed" in w.lower() for w in response._meta.warnings)

    @pytest.mark.asyncio
    async def test_llm_called_when_no_rule_match(self) -> None:
        from kuberca.llm.analyzer import LLMResult
        from kuberca.models.analysis import QualityCheckResult

        llm_result = LLMResult(
            success=True,
            root_cause="Image pull failed due to wrong tag",
            confidence=0.55,
            evidence_citations=["2026-02-18T12:00:00.000Z OOMKilled"],
            affected_resources=[AffectedResource("Pod", "default", "my-pod")],
            suggested_remediation="Fix image tag",
            causal_chain="The image tag was changed...",
        )
        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=llm_result)
        # quality_check is a sync method — use MagicMock, not AsyncMock
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=True, failures=[]),
        )

        coordinator = _make_coordinator(
            rule_result=None,
            llm_analyzer=llm_analyzer,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.LLM
        assert response.confidence == pytest.approx(0.55, abs=0.01)
        assert response.rule_id is None

    @pytest.mark.asyncio
    async def test_llm_unavailable_returns_inconclusive(self) -> None:
        llm_analyzer = MagicMock()
        llm_analyzer.available = False

        coordinator = _make_coordinator(
            rule_result=None,
            llm_analyzer=llm_analyzer,
        )
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        assert response._meta is not None
        assert any("llm unavailable" in w.lower() for w in response._meta.warnings)

    @pytest.mark.asyncio
    async def test_invalid_resource_format_returns_inconclusive(self) -> None:
        coordinator = _make_coordinator()
        response = await coordinator.analyze("not-valid")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE

    @pytest.mark.asyncio
    async def test_meta_populated_correctly(self) -> None:
        rule_result = _make_rule_result()
        coordinator = _make_coordinator(rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response._meta is not None
        from kuberca import __version__

        assert response._meta.kuberca_version == __version__
        assert response._meta.schema_version == "1"
        assert response._meta.cluster_id == "test-cluster"
        assert response._meta.cache_state == "ready"
        assert response._meta.response_time_ms >= 0

    @pytest.mark.asyncio
    async def test_evidence_sorted_by_timestamp_descending(self) -> None:
        rule_result = RuleResult(
            rule_id="R01_oom_killed",
            root_cause="OOM",
            confidence=0.75,
            evidence=[
                EvidenceItem(EvidenceType.EVENT, "2026-02-18T10:00:00.000Z", "earlier"),
                EvidenceItem(EvidenceType.EVENT, "2026-02-18T12:00:00.000Z", "later"),
                EvidenceItem(EvidenceType.EVENT, "2026-02-18T11:00:00.000Z", "middle"),
            ],
            affected_resources=[],
            suggested_remediation="",
        )
        coordinator = _make_coordinator(rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        timestamps = [e.timestamp for e in response.evidence]
        assert timestamps == sorted(timestamps, reverse=True)

    @pytest.mark.asyncio
    async def test_affected_resources_deduplicated(self) -> None:
        dup_resource = AffectedResource("Pod", "default", "my-pod")
        rule_result = RuleResult(
            rule_id="R01_oom_killed",
            root_cause="OOM",
            confidence=0.75,
            evidence=[],
            affected_resources=[dup_resource, dup_resource, dup_resource],
            suggested_remediation="",
        )
        coordinator = _make_coordinator(rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert len(response.affected_resources) == 1
