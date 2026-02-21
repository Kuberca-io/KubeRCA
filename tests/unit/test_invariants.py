"""Invariant test suite for KubeRCA.

Tests that critical system invariants hold under normal and adversarial
conditions.  Each test class maps to a specific invariant ID documented in
INVARIANTS.md and enforced at runtime where possible.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from kuberca.cache.resource_cache import ResourceCache
from kuberca.llm.analyzer import _compute_confidence as llm_compute_confidence
from kuberca.models.analysis import (
    AffectedResource,
    CorrelationResult,
    EvaluationMeta,
    EvidenceItem,
    RuleResult,
)
from kuberca.models.events import EventRecord, EventSource, EvidenceType, Severity
from kuberca.models.resources import CachedResourceView, CacheReadiness, FieldChange
from kuberca.rules.base import ChangeLedger, Rule, RuleEngine
from kuberca.rules.base import ResourceCache as ResourceCacheProto
from kuberca.rules.confidence import compute_confidence

# ---------------------------------------------------------------------------
# Shared timestamps & helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 2, 21, 12, 0, 0, tzinfo=UTC)
_15_MIN_AGO = _NOW - timedelta(minutes=15)


def _make_event(
    reason: str = "OOMKilled",
    count: int = 1,
    last_seen: datetime | None = None,
) -> EventRecord:
    seen = last_seen or _NOW
    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=Severity.ERROR,
        reason=reason,
        message="test event",
        namespace="default",
        resource_kind="Pod",
        resource_name="my-pod",
        first_seen=seen - timedelta(minutes=1),
        last_seen=seen,
        count=count,
    )


def _make_change(
    minutes_ago: int = 10,
    field_path: str = "spec.template.spec.containers[0].resources.limits.memory",
) -> FieldChange:
    return FieldChange(
        field_path=field_path,
        old_value="256Mi",
        new_value="512Mi",
        changed_at=_NOW - timedelta(minutes=minutes_ago),
    )


def _make_view(
    kind: str = "Deployment",
    namespace: str = "default",
    name: str = "my-deploy",
) -> CachedResourceView:
    return CachedResourceView(
        kind=kind,
        namespace=namespace,
        name=name,
        resource_version="1",
        labels={},
        annotations={},
        spec={},
        status={},
        last_updated=_NOW,
    )


class _StubRule(Rule):
    """Minimal rule stub for tests."""

    def __init__(
        self,
        rule_id: str = "R_TEST",
        priority: int = 10,
        base_confidence: float = 0.55,
        resource_dependencies: list[str] | None = None,
        match_result: bool = True,
        explain_confidence: float | None = None,
    ) -> None:
        self.rule_id = rule_id
        self.display_name = rule_id
        self.priority = priority
        self.base_confidence = base_confidence
        self.resource_dependencies = resource_dependencies or ["Pod"]
        self.relevant_field_paths = ["spec."]
        self._match_result = match_result
        self._explain_confidence = explain_confidence

    def match(self, event: EventRecord) -> bool:
        return self._match_result

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCacheProto,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        return CorrelationResult(objects_queried=1, duration_ms=1.0)

    def explain(self, event: EventRecord, correlation: CorrelationResult) -> RuleResult:
        confidence = self._explain_confidence if self._explain_confidence is not None else self.base_confidence
        return RuleResult(
            rule_id=self.rule_id,
            root_cause="test root cause",
            confidence=confidence,
            evidence=[
                EvidenceItem(
                    type=EvidenceType.EVENT,
                    timestamp=_NOW.isoformat(),
                    summary="test evidence",
                )
            ],
            affected_resources=[AffectedResource(kind="Pod", namespace="default", name="my-pod")],
            suggested_remediation="test remediation",
        )


def _make_mock_cache() -> MagicMock:
    cache = MagicMock(spec=ResourceCacheProto)
    cache.get.return_value = None
    cache.list.return_value = []
    cache.list_by_label.return_value = []
    cache.list_truncated.return_value = False
    return cache


def _make_mock_ledger() -> MagicMock:
    ledger = MagicMock(spec=ChangeLedger)
    ledger.diff.return_value = []
    ledger.latest.return_value = None
    return ledger


# ===========================================================================
# INV-C01: Confidence Score Range [0.0, 0.95]
# ===========================================================================


class TestConfidenceScoreRange:
    """INV-C01: compute_confidence() returns values in [0.0, 0.95]."""

    def test_minimum_confidence_is_non_negative(self) -> None:
        rule = _StubRule(base_confidence=0.0)
        event = _make_event(count=1)
        corr = CorrelationResult(objects_queried=0, duration_ms=0.0)
        result = compute_confidence(rule, corr, event)
        assert result >= 0.0

    def test_maximum_confidence_capped_at_095(self) -> None:
        rule = _StubRule(base_confidence=0.95)
        event = _make_event(count=5)
        changes = [_make_change(minutes_ago=5)]
        views = [_make_view()]
        corr = CorrelationResult(
            changes=changes,
            related_resources=views[:1],
            objects_queried=1,
            duration_ms=1.0,
        )
        result = compute_confidence(rule, corr, event)
        assert result <= 0.95

    def test_all_bonuses_capped_at_095(self) -> None:
        """Even with max base and all bonuses, confidence stays at 0.95."""
        rule = _StubRule(base_confidence=0.80)
        event = _make_event(count=5)
        changes = [_make_change(minutes_ago=5)]
        corr = CorrelationResult(
            changes=changes,
            related_resources=[_make_view()],
            objects_queried=1,
            duration_ms=1.0,
        )
        result = compute_confidence(rule, corr, event)
        assert result == 0.95


# ===========================================================================
# INV-CS01: Cache State Transitions
# ===========================================================================


class TestCacheStateTransitions:
    """INV-CS01: Only allowed state transitions occur in the cache."""

    def test_warming_to_partially_ready(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Node"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.PARTIALLY_READY

    def test_partially_ready_to_ready(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Node"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.PARTIALLY_READY

        cache._ready_kinds = {"Pod", "Node"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

    def test_ready_to_degraded(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

        # Force divergence via reconnect failures
        cache._reconnect_failures = 10
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.DEGRADED

    def test_degraded_to_ready(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        # Go to READY first
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY
        # Go to DEGRADED
        cache._reconnect_failures = 10
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.DEGRADED
        # Recover
        cache._reconnect_failures = 0
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

    def test_noop_no_counter(self) -> None:
        """When readiness does not change, no transition counter is incremented."""
        from prometheus_client import Counter

        counter = Counter(
            "kuberca_cache_state_transitions_noop_test",
            "Test counter",
            ["from_state", "to_state"],
            registry=None,
        )
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        # Now call again — state is still READY, no transition
        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        # The warming->ready transition happens once; second call is no-op
        val = counter.labels(from_state="warming", to_state="ready")._value.get()
        assert val == 1.0


# ===========================================================================
# INV-C02: Coordinator Final Confidence Range
# ===========================================================================


class TestCoordinatorConfidenceRange:
    """INV-C02: Final confidence after cache penalties stays in [0.0, 0.95]."""

    def test_degraded_penalty_stays_positive(self) -> None:
        """Confidence after DEGRADED penalty (−0.10) remains >= 0.0."""
        from kuberca.analyst.coordinator import _rule_result_to_rca_response

        rule_result = RuleResult(
            rule_id="R_TEST",
            root_cause="test",
            confidence=0.05,
        )
        eval_meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=1.0)
        start_ms = 0

        response = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.DEGRADED,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=start_ms,
        )
        assert response.confidence >= 0.0

    def test_partially_ready_penalty(self) -> None:
        """Confidence after PARTIALLY_READY penalty (−0.15) remains >= 0.0."""
        from kuberca.analyst.coordinator import _rule_result_to_rca_response

        rule_result = RuleResult(
            rule_id="R_TEST",
            root_cause="test",
            confidence=0.10,
        )
        eval_meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=1.0)

        response = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.PARTIALLY_READY,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        assert response.confidence >= 0.0

    def test_ready_no_penalty(self) -> None:
        """READY state applies no confidence penalty."""
        from kuberca.analyst.coordinator import _rule_result_to_rca_response

        rule_result = RuleResult(
            rule_id="R_TEST",
            root_cause="test",
            confidence=0.80,
        )
        eval_meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=1.0)

        response = _rule_result_to_rca_response(
            rule_result=rule_result,
            cache_readiness=CacheReadiness.READY,
            eval_meta=eval_meta,
            cluster_id="test",
            start_ms=0,
        )
        assert response.confidence == 0.80


# ===========================================================================
# INV-C03: Rule Engine Evaluation Order
# ===========================================================================


class TestRuleEngineEvaluationOrder:
    """INV-C03: Rules are always evaluated in (priority, rule_id) order."""

    def test_sorted_after_registration(self) -> None:
        cache = _make_mock_cache()
        ledger = _make_mock_ledger()
        engine = RuleEngine(cache, ledger)

        r1 = _StubRule(rule_id="R01", priority=10)
        r2 = _StubRule(rule_id="R02", priority=5)
        r3 = _StubRule(rule_id="R03", priority=10)

        engine.register(r1)
        engine.register(r2)
        engine.register(r3)

        rule_ids = [r.rule_id for r in engine._rules]
        assert rule_ids == ["R02", "R01", "R03"]

    def test_stable_after_multiple_registrations(self) -> None:
        cache = _make_mock_cache()
        ledger = _make_mock_ledger()
        engine = RuleEngine(cache, ledger)

        rules = [
            _StubRule(rule_id="R05", priority=20),
            _StubRule(rule_id="R01", priority=10),
            _StubRule(rule_id="R03", priority=10),
            _StubRule(rule_id="R02", priority=5),
            _StubRule(rule_id="R04", priority=15),
        ]
        for r in rules:
            engine.register(r)

        rule_ids = [r.rule_id for r in engine._rules]
        assert rule_ids == ["R02", "R01", "R03", "R04", "R05"]

        # Re-registering should maintain order
        engine.register(_StubRule(rule_id="R00", priority=1))
        rule_ids = [r.rule_id for r in engine._rules]
        assert rule_ids[0] == "R00"


# ===========================================================================
# INV-C04: Confidence Pure Function
# ===========================================================================


class TestConfidencePureFunctionInvariant:
    """INV-C04: compute_confidence is a pure function — same inputs, same output."""

    def test_1000_identical_calls_same_output(self) -> None:
        rule = _StubRule(base_confidence=0.55)
        event = _make_event(count=3)
        changes = [_make_change(minutes_ago=10)]
        corr = CorrelationResult(
            changes=changes,
            related_resources=[_make_view()],
            objects_queried=1,
            duration_ms=1.0,
        )

        first_result = compute_confidence(rule, corr, event)
        for _ in range(999):
            assert compute_confidence(rule, corr, event) == first_result


# ===========================================================================
# INV-C05: LLM Confidence Cap
# ===========================================================================


class TestLLMConfidenceCapInvariant:
    """INV-C05: LLM confidence is always capped at 0.70."""

    def test_llm_capped_at_070_with_max_citations(self) -> None:
        """Even with many citations and diff support, cap is 0.70."""
        result = llm_compute_confidence(
            citations=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            causal_chain="detailed chain",
            has_ledger_diff_support=True,
        )
        assert result <= 0.70

    def test_llm_capped_at_060_without_diff(self) -> None:
        """Without diff support, cap is 0.60."""
        result = llm_compute_confidence(
            citations=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            causal_chain="detailed chain",
            has_ledger_diff_support=False,
        )
        assert result <= 0.60

    def test_llm_zero_citations_low(self) -> None:
        """Zero citations stays in [0.30, 0.40] range."""
        result = llm_compute_confidence(
            citations=[],
            causal_chain="",
            has_ledger_diff_support=True,
        )
        assert 0.30 <= result <= 0.40


# ===========================================================================
# INV-C06 through C08: Four-Band Invariants
# ===========================================================================


class TestFourBandInvariants:
    """INV-C06–C08: 4-band strategy works correctly."""

    def test_high_band_short_circuits(self) -> None:
        """INV-C06: Confidence >= 0.85 short-circuits; no further rules evaluated."""
        cache = _make_mock_cache()
        ledger = _make_mock_ledger()
        engine = RuleEngine(cache, ledger)

        # First rule returns high confidence
        r_high = _StubRule(rule_id="R01", priority=10, explain_confidence=0.90)
        r_low = _StubRule(rule_id="R02", priority=20, explain_confidence=0.50)
        engine.register(r_high)
        engine.register(r_low)

        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is not None
        assert result.rule_id == "R01"
        assert result.confidence >= 0.85
        # Only 1 rule matched (short-circuited before R02)
        assert meta.rules_matched == 1

    def test_medium_restricts_shared_deps(self) -> None:
        """INV-C07: Medium-band (0.70-0.84) enters competing-deps-only mode."""
        cache = _make_mock_cache()
        ledger = _make_mock_ledger()
        engine = RuleEngine(cache, ledger)

        # R01 returns medium confidence, depends on "Pod"
        r_med = _StubRule(
            rule_id="R01",
            priority=10,
            explain_confidence=0.75,
            resource_dependencies=["Pod"],
        )
        # R02 depends on "Node" — no overlap with "Pod"
        r_no_overlap = _StubRule(
            rule_id="R02",
            priority=20,
            explain_confidence=0.80,
            resource_dependencies=["Node"],
        )
        # R03 depends on "Pod" — overlaps
        r_overlap = _StubRule(
            rule_id="R03",
            priority=30,
            explain_confidence=0.80,
            resource_dependencies=["Pod"],
        )

        engine.register(r_med)
        engine.register(r_no_overlap)
        engine.register(r_overlap)

        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is not None
        # R03 has higher confidence and shares deps, so it wins
        assert result.rule_id == "R03"
        assert result.confidence == 0.80

    def test_low_evaluates_all(self) -> None:
        """INV-C08: Low-band (< 0.70) evaluates all remaining rules."""
        cache = _make_mock_cache()
        ledger = _make_mock_ledger()
        engine = RuleEngine(cache, ledger)

        r_low = _StubRule(
            rule_id="R01",
            priority=10,
            explain_confidence=0.50,
            resource_dependencies=["Pod"],
        )
        r_other = _StubRule(
            rule_id="R02",
            priority=20,
            explain_confidence=0.60,
            resource_dependencies=["Node"],
        )

        engine.register(r_low)
        engine.register(r_other)

        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is not None
        # R02 has higher confidence, even though no shared deps
        assert result.rule_id == "R02"
        assert result.confidence == 0.60


# ===========================================================================
# INV-C09: Competing Deps Only in Medium Band
# ===========================================================================


class TestCompetingDepsOnlyMediumBand:
    """INV-C09: Low confidence does NOT activate competing-deps-only mode."""

    def test_low_confidence_does_not_activate_competing_mode(self) -> None:
        cache = _make_mock_cache()
        ledger = _make_mock_ledger()
        engine = RuleEngine(cache, ledger)

        r_low = _StubRule(
            rule_id="R01",
            priority=10,
            explain_confidence=0.50,
            resource_dependencies=["Pod"],
        )
        # R02 has different deps — if competing mode were active, it would be skipped
        r_different_deps = _StubRule(
            rule_id="R02",
            priority=20,
            explain_confidence=0.65,
            resource_dependencies=["Node"],
        )

        engine.register(r_low)
        engine.register(r_different_deps)

        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is not None
        # R02 evaluated and won because low band doesn't restrict
        assert result.rule_id == "R02"


# ===========================================================================
# INV-DT01 through DT04: Divergence Trigger Invariants
# ===========================================================================


class TestDivergenceTriggerInvariants:
    """INV-DT01 through DT04: Cache divergence triggers."""

    def test_miss_rate_above_threshold_degrades(self) -> None:
        """INV-DT01: Miss rate > 5% triggers DEGRADED."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

        # Simulate 10% miss rate: 10 misses, 90 hits
        # _access_log stores (datetime, hit) where hit=True means CACHE HIT
        now = datetime.now(tz=UTC)
        for _ in range(10):
            cache._access_log.append((now, False))  # misses
        for _ in range(90):
            cache._access_log.append((now, True))  # hits

        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.DEGRADED

    def test_miss_rate_below_threshold_stays_ready(self) -> None:
        """INV-DT01 inverse: miss rate <= 5% does NOT trigger DEGRADED."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

        now = datetime.now(tz=UTC)
        # 4% miss rate: 4 misses, 96 hits
        for _ in range(4):
            cache._access_log.append((now, False))
        for _ in range(96):
            cache._access_log.append((now, True))

        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

    def test_reconnect_failures_above_threshold_degrades(self) -> None:
        """INV-DT02: > 3 reconnect failures triggers DEGRADED."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

        cache._reconnect_failures = 4
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.DEGRADED

    def test_reconnect_failures_at_threshold_stays_ready(self) -> None:
        """INV-DT02 inverse: exactly 3 reconnect failures does NOT degrade."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()

        cache._reconnect_failures = 3
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

    def test_relist_timeouts_above_threshold_degrades(self) -> None:
        """INV-DT03: > 2 relist timeouts in 10 min triggers DEGRADED."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

        now = datetime.now(tz=UTC)
        for _ in range(3):
            cache._relist_timeout_times.append(now)

        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.DEGRADED

    def test_burst_errors_above_threshold_degrades(self) -> None:
        """INV-DT04: Burst errors lasting > 1 minute trigger DEGRADED.

        The ``_recent_burst_duration`` method prunes entries older than
        ``_BURST_WINDOW`` before measuring the span, so we patch it to
        simulate a duration that exceeds the threshold.
        """
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.READY

        # Patch _recent_burst_duration to return > 60s (the _BURST_WINDOW)
        with patch.object(cache, "_recent_burst_duration", return_value=61.0):
            cache._recompute_readiness()
        assert cache._readiness == CacheReadiness.DEGRADED


# ===========================================================================
# INV-A01 through A04: Analysis Pipeline Invariants
# ===========================================================================


class TestAnalysisInvariants:
    """INV-A01 through A04: Analysis pipeline safety bounds."""

    def test_correlate_timeout_500ms(self) -> None:
        """INV-A01: Correlate timeout is 500ms."""
        from kuberca.rules.base import _CORRELATE_TIMEOUT_MS

        assert _CORRELATE_TIMEOUT_MS == 500.0

    def test_max_objects_50(self) -> None:
        """INV-A02: Max objects queried per correlate is 50."""
        from kuberca.rules.base import _MAX_OBJECTS

        assert _MAX_OBJECTS == 50

    def test_llm_retry_cap_2(self) -> None:
        """INV-A03: LLM quality retry cap is 2."""
        # The retry cap is defined as a local variable in
        # AnalystCoordinator.analyze(). We verify by inspecting source.
        import inspect

        from kuberca.analyst.coordinator import AnalystCoordinator

        source = inspect.getsource(AnalystCoordinator.analyze)
        assert "max_quality_retries = 2" in source

    def test_quality_failure_cap_035(self) -> None:
        """INV-A04: Quality failure caps confidence at 0.35."""
        import inspect

        from kuberca.analyst.coordinator import AnalystCoordinator

        source = inspect.getsource(AnalystCoordinator.analyze)
        assert "min(llm_result.confidence, 0.35)" in source
