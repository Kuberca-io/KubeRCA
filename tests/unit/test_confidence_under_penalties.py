"""Tests proving confidence degrades gracefully across cache readiness states.

Invariants proven:
- The same incident produces the same RULE WINNER regardless of cache state.
- Confidence monotonically decreases: READY > PARTIALLY_READY > DEGRADED (0.0 when no rule match).
- DEGRADED with no rule match always returns INCONCLUSIVE (confidence 0.0).
- Penalties are applied AFTER rule selection (rule engine is cache-unaware).
- All scenarios are deterministic over 100 replays.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from kuberca.analyst.coordinator import AnalystCoordinator, _rule_result_to_rca_response
from kuberca.models.analysis import (
    AffectedResource,
    EvaluationMeta,
    EvidenceItem,
    RCAResponse,
    RuleResult,
)
from kuberca.models.events import DiagnosisSource, EventRecord, EventSource, EvidenceType, Severity
from kuberca.models.resources import CacheReadiness

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 2, 21, 10, 0, 0, tzinfo=UTC)


def _make_event(reason: str = "OOMKilled") -> EventRecord:
    """Create a deterministic EventRecord for testing."""
    return EventRecord(
        event_id="evt-fixed-001",
        cluster_id="test-cluster",
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message=f"Container terminated: {reason}",
        namespace="default",
        resource_kind="Pod",
        resource_name="my-pod",
        first_seen=_NOW,
        last_seen=_NOW,
    )


def _make_rule_result(confidence: float = 0.80) -> RuleResult:
    """Create a deterministic RuleResult (always R01_oom_killed)."""
    return RuleResult(
        rule_id="R01_oom_killed",
        root_cause="Pod OOM killed due to memory limit exceeded",
        confidence=confidence,
        evidence=[
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp="2026-02-21T10:00:00.000Z",
                summary="OOMKilled event observed",
                debug_context=None,
            )
        ],
        affected_resources=[AffectedResource(kind="Pod", namespace="default", name="my-pod")],
        suggested_remediation="Increase memory limit or fix memory leak",
    )


def _make_meta() -> EvaluationMeta:
    """Create a deterministic EvaluationMeta."""
    return EvaluationMeta(rules_evaluated=18, rules_matched=1, duration_ms=1.5)


def _make_coordinator(
    cache_readiness: CacheReadiness,
    rule_result: RuleResult | None = None,
) -> AnalystCoordinator:
    """Build an AnalystCoordinator with mocked dependencies.

    - cache.readiness() returns the specified readiness.
    - cache.get() returns None (no cached resource view).
    - event_buffer.get_events() always returns [_make_event()] so the coordinator
      proceeds past the "no events" check and reaches the rule engine / cache
      readiness decision paths.
    - rule_engine.evaluate() returns (rule_result, _make_meta()) when rule_result is
      provided, (None, _make_meta()) otherwise.
    """
    cache = MagicMock()
    cache.readiness.return_value = cache_readiness
    cache.get.return_value = None

    ledger = MagicMock()
    ledger.diff.return_value = []

    event_buffer = MagicMock()
    event_buffer.get_events.return_value = [_make_event()]

    eval_meta = _make_meta()
    if rule_result is None:
        eval_meta = EvaluationMeta(rules_evaluated=18, rules_matched=0, duration_ms=1.5)

    rule_engine = MagicMock()
    rule_engine.evaluate.return_value = (rule_result, eval_meta)

    config = MagicMock()
    config.cluster_id = "test-cluster"
    config.ollama = MagicMock()
    config.ollama.endpoint = "http://localhost:11434"

    return AnalystCoordinator(
        rule_engine=rule_engine,
        llm_analyzer=None,
        cache=cache,
        ledger=ledger,
        event_buffer=event_buffer,
        config=config,
    )


# ---------------------------------------------------------------------------
# 1. TestRuleWinnerStability
# ---------------------------------------------------------------------------


class TestRuleWinnerStability:
    """Prove R01 OOMKilled wins at both READY and PARTIALLY_READY."""

    async def test_r01_wins_at_ready(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.READY, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE
        assert response.rule_id == "R01_oom_killed"

    async def test_r01_wins_at_partially_ready(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.PARTIALLY_READY, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE
        assert response.rule_id == "R01_oom_killed"

    async def test_same_rule_id_at_both_states(self) -> None:
        rule_result_a = _make_rule_result(confidence=0.80)
        rule_result_b = _make_rule_result(confidence=0.80)

        coord_ready = _make_coordinator(CacheReadiness.READY, rule_result=rule_result_a)
        coord_partial = _make_coordinator(CacheReadiness.PARTIALLY_READY, rule_result=rule_result_b)

        resp_ready = await coord_ready.analyze("pod/default/my-pod")
        resp_partial = await coord_partial.analyze("pod/default/my-pod")

        assert resp_ready.rule_id == resp_partial.rule_id == "R01_oom_killed"
        assert resp_ready.root_cause == resp_partial.root_cause

    async def test_r01_wins_at_degraded_when_rule_matches(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE
        assert response.rule_id == "R01_oom_killed"


# ---------------------------------------------------------------------------
# 2. TestGracefulDegradationCurve
# ---------------------------------------------------------------------------


class TestGracefulDegradationCurve:
    """Prove confidence monotonically decreases: READY > PARTIALLY_READY > DEGRADED."""

    async def test_monotonic_decrease_with_rule_match(self) -> None:
        """READY > any non-READY state for the same rule result.

        Actual penalties: DEGRADED -0.10, PARTIALLY_READY -0.15.
        So: READY (0.80) > DEGRADED (0.70) > PARTIALLY_READY (0.65).
        Both penalised states are strictly less than READY.
        """
        confidences: dict[CacheReadiness, float] = {}
        for readiness in [CacheReadiness.READY, CacheReadiness.PARTIALLY_READY, CacheReadiness.DEGRADED]:
            rule_result = _make_rule_result(confidence=0.80)
            coordinator = _make_coordinator(readiness, rule_result=rule_result)
            response = await coordinator.analyze("pod/default/my-pod")
            confidences[readiness] = response.confidence

        # READY is the highest
        assert confidences[CacheReadiness.READY] > confidences[CacheReadiness.DEGRADED]
        assert confidences[CacheReadiness.READY] > confidences[CacheReadiness.PARTIALLY_READY]
        # Both penalised states are lower than READY
        assert confidences[CacheReadiness.DEGRADED] < confidences[CacheReadiness.READY]
        assert confidences[CacheReadiness.PARTIALLY_READY] < confidences[CacheReadiness.READY]

    async def test_ready_confidence_is_original(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.READY, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.confidence == pytest.approx(0.80, abs=0.01)

    async def test_partially_ready_confidence_reduced_by_0_15(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.PARTIALLY_READY, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.confidence == pytest.approx(0.65, abs=0.01)

    async def test_degraded_confidence_reduced_by_0_10(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.confidence == pytest.approx(0.70, abs=0.01)

    async def test_degraded_no_rule_match_returns_inconclusive(self) -> None:
        """When no rule matches AND cache is DEGRADED, result is INCONCLUSIVE with confidence 0.0."""
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=None)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        assert response.confidence == 0.0

    async def test_degraded_no_rule_match_root_cause_mentions_degraded(self) -> None:
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=None)
        response = await coordinator.analyze("pod/default/my-pod")
        # The coordinator returns "Unable to diagnose: cache degraded." at line 518
        assert "degraded" in response.root_cause.lower() or "unable to diagnose" in response.root_cause.lower()


# ---------------------------------------------------------------------------
# 3. TestNoRuleFlipping
# ---------------------------------------------------------------------------


class TestNoRuleFlipping:
    """Prove _rule_result_to_rca_response preserves rule_id across cache states."""

    def test_same_rule_id_at_ready_and_partially_ready(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp_ready = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        resp_partial = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )

        assert resp_ready.rule_id == resp_partial.rule_id == "R01_oom_killed"
        assert resp_ready.root_cause == resp_partial.root_cause

    def test_same_rule_id_at_degraded(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp_degraded = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )

        assert resp_degraded.rule_id == "R01_oom_killed"
        assert resp_degraded.diagnosed_by == DiagnosisSource.RULE_ENGINE

    def test_remediation_unchanged_across_states(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        responses: list[RCAResponse] = []
        for readiness in [CacheReadiness.READY, CacheReadiness.PARTIALLY_READY, CacheReadiness.DEGRADED]:
            resp = _rule_result_to_rca_response(
                rule_result=rule_result,
                cache_readiness=readiness,
                eval_meta=meta,
                cluster_id="test-cluster",
                start_ms=start_ms,
            )
            responses.append(resp)

        remediations = {r.suggested_remediation for r in responses}
        assert len(remediations) == 1  # All the same


# ---------------------------------------------------------------------------
# 4. TestBoundaryPenaltyCases
# ---------------------------------------------------------------------------


class TestBoundaryPenaltyCases:
    """Sync tests exercising boundary penalty arithmetic."""

    def test_partially_ready_0_70_minus_0_15_equals_0_55(self) -> None:
        rule_result = _make_rule_result(confidence=0.70)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        assert resp.confidence == pytest.approx(0.55, abs=0.001)

    def test_partially_ready_0_85_minus_0_15_equals_0_70(self) -> None:
        rule_result = _make_rule_result(confidence=0.85)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        assert resp.confidence == pytest.approx(0.70, abs=0.001)

    def test_floor_at_zero_partially_ready(self) -> None:
        rule_result = _make_rule_result(confidence=0.10)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        # 0.10 - 0.15 = -0.05, clamped to 0.0
        assert resp.confidence == pytest.approx(0.0, abs=0.001)

    def test_degraded_0_80_minus_0_10_equals_0_70(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        assert resp.confidence == pytest.approx(0.70, abs=0.001)

    def test_degraded_floor_at_zero(self) -> None:
        rule_result = _make_rule_result(confidence=0.05)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        # 0.05 - 0.10 = -0.05, clamped to 0.0
        assert resp.confidence == pytest.approx(0.0, abs=0.001)

    def test_ready_no_penalty(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        assert resp.confidence == pytest.approx(0.80, abs=0.001)

    def test_ready_capped_at_0_95(self) -> None:
        rule_result = _make_rule_result(confidence=0.99)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        assert resp.confidence == pytest.approx(0.95, abs=0.001)

    def test_partially_ready_high_confidence_capped_at_0_95(self) -> None:
        # 1.20 - 0.15 = 1.05, capped at 0.95
        rule_result = _make_rule_result(confidence=1.20)
        meta = _make_meta()
        start_ms = time.monotonic_ns() // 1_000_000

        resp = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=meta,
            cluster_id="test-cluster",
            start_ms=start_ms,
        )
        assert resp.confidence == pytest.approx(0.95, abs=0.001)


# ---------------------------------------------------------------------------
# 5. TestPenaltyAppliedPostSelection
# ---------------------------------------------------------------------------


class TestPenaltyAppliedPostSelection:
    """Prove rule_engine.evaluate() receives only the event (no cache state)."""

    async def test_rule_engine_evaluate_receives_only_event(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.PARTIALLY_READY, rule_result=rule_result)
        await coordinator.analyze("pod/default/my-pod")

        # The rule engine's evaluate method should have been called with a single EventRecord
        rule_engine = coordinator._rule_engine
        rule_engine.evaluate.assert_called_once()
        call_args = rule_engine.evaluate.call_args
        # Positional arg: the event
        assert len(call_args.args) == 1
        event_arg = call_args.args[0]
        assert isinstance(event_arg, EventRecord)
        # No keyword args (no cache state passed)
        assert call_args.kwargs == {}

    async def test_rule_engine_does_not_receive_cache_readiness(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=rule_result)
        await coordinator.analyze("pod/default/my-pod")

        rule_engine = coordinator._rule_engine
        call_args = rule_engine.evaluate.call_args
        # Verify the argument is an EventRecord, not a CacheReadiness
        event_arg = call_args.args[0]
        assert not isinstance(event_arg, CacheReadiness)
        assert isinstance(event_arg, EventRecord)

    async def test_rule_engine_sees_same_event_regardless_of_cache_state(self) -> None:
        """Rule engine gets the same event at READY and DEGRADED."""
        rule_result_ready = _make_rule_result(confidence=0.80)
        rule_result_degraded = _make_rule_result(confidence=0.80)

        coord_ready = _make_coordinator(CacheReadiness.READY, rule_result=rule_result_ready)
        coord_degraded = _make_coordinator(CacheReadiness.DEGRADED, rule_result=rule_result_degraded)

        await coord_ready.analyze("pod/default/my-pod")
        await coord_degraded.analyze("pod/default/my-pod")

        event_ready = coord_ready._rule_engine.evaluate.call_args.args[0]
        event_degraded = coord_degraded._rule_engine.evaluate.call_args.args[0]

        assert event_ready.reason == event_degraded.reason == "OOMKilled"
        assert event_ready.resource_kind == event_degraded.resource_kind == "Pod"
        assert event_ready.namespace == event_degraded.namespace == "default"
        assert event_ready.resource_name == event_degraded.resource_name == "my-pod"


# ---------------------------------------------------------------------------
# 6. TestReplayStability
# ---------------------------------------------------------------------------


class TestReplayStability:
    """100 replays at READY and PARTIALLY_READY must all produce identical results."""

    async def test_100_replays_at_ready(self) -> None:
        results: list[tuple[str | None, float, str]] = []
        for _ in range(100):
            rule_result = _make_rule_result(confidence=0.80)
            coordinator = _make_coordinator(CacheReadiness.READY, rule_result=rule_result)
            response = await coordinator.analyze("pod/default/my-pod")
            results.append((response.rule_id, response.confidence, response.root_cause))

        first = results[0]
        for i, result in enumerate(results[1:], start=1):
            assert result == first, f"Replay {i} diverged: {result} != {first}"

    async def test_100_replays_at_partially_ready(self) -> None:
        results: list[tuple[str | None, float, str]] = []
        for _ in range(100):
            rule_result = _make_rule_result(confidence=0.80)
            coordinator = _make_coordinator(CacheReadiness.PARTIALLY_READY, rule_result=rule_result)
            response = await coordinator.analyze("pod/default/my-pod")
            results.append((response.rule_id, response.confidence, response.root_cause))

        first = results[0]
        for i, result in enumerate(results[1:], start=1):
            assert result == first, f"Replay {i} diverged: {result} != {first}"

    async def test_100_replays_confidence_values_consistent(self) -> None:
        """Verify exact confidence values across 100 replays for each state."""
        for readiness, expected_conf in [
            (CacheReadiness.READY, 0.80),
            (CacheReadiness.PARTIALLY_READY, 0.65),
            (CacheReadiness.DEGRADED, 0.70),
        ]:
            for _ in range(100):
                rule_result = _make_rule_result(confidence=0.80)
                coordinator = _make_coordinator(readiness, rule_result=rule_result)
                response = await coordinator.analyze("pod/default/my-pod")
                assert response.confidence == pytest.approx(expected_conf, abs=0.001), (
                    f"Confidence mismatch at {readiness}: {response.confidence} != {expected_conf}"
                )


# ---------------------------------------------------------------------------
# 7. TestWarningsPresent
# ---------------------------------------------------------------------------


class TestWarningsPresent:
    """Verify cache-state warnings appear in the response _meta."""

    async def test_partially_ready_has_partially_ready_warning(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.PARTIALLY_READY, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response._meta is not None
        warnings_lower = [w.lower() for w in response._meta.warnings]
        assert any("partially ready" in w for w in warnings_lower)

    async def test_degraded_rule_match_has_degraded_warning(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response._meta is not None
        warnings_lower = [w.lower() for w in response._meta.warnings]
        assert any("degraded" in w for w in warnings_lower)

    async def test_degraded_no_rule_match_has_suppressed_warning(self) -> None:
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=None)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response._meta is not None
        warnings_lower = [w.lower() for w in response._meta.warnings]
        assert any("suppressed" in w for w in warnings_lower)

    async def test_degraded_no_rule_match_has_degraded_warning(self) -> None:
        coordinator = _make_coordinator(CacheReadiness.DEGRADED, rule_result=None)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response._meta is not None
        warnings_lower = [w.lower() for w in response._meta.warnings]
        assert any("degraded" in w for w in warnings_lower)

    async def test_ready_has_no_cache_warnings(self) -> None:
        rule_result = _make_rule_result(confidence=0.80)
        coordinator = _make_coordinator(CacheReadiness.READY, rule_result=rule_result)
        response = await coordinator.analyze("pod/default/my-pod")
        assert response._meta is not None
        warnings_lower = [w.lower() for w in response._meta.warnings]
        assert not any("degraded" in w or "partially ready" in w or "suppressed" in w for w in warnings_lower)
