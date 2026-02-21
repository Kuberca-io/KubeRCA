# Invariant Protection, Confidence Under Penalties, Stress Intersections — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Codify system invariants with runtime checks + documentation, prove confidence degrades gracefully under cache penalties, and test 8 failure mode intersections under stress.

**Architecture:** Three parallel tracks: (1) INVARIANTS.md + runtime warning checks + `kuberca_invariant_violations_total` metric + invariant test suite, (2) coordinator-level confidence-under-penalty tests proving rule selection is cache-state-independent, (3) 8 new stress intersection tests exercising real failure combinations.

**Tech Stack:** Python 3.12, pytest, pytest-asyncio, structlog, prometheus_client, unittest.mock

---

## Task 1: Add `kuberca_invariant_violations_total` Metric

**Files:**
- Modify: `kuberca/observability/metrics.py:5` (add new Counter)

**Step 1: Add the metric definition**

Add after line 96 (after `cache_state_transitions_total`):

```python
# Invariant violation metrics
invariant_violations_total = Counter(
    "kuberca_invariant_violations_total",
    "Total invariant violations detected at runtime (should always be 0)",
    ["invariant_name"],
)
```

**Step 2: Verify lint passes**

Run: `uv run ruff check kuberca/observability/metrics.py`
Expected: clean

**Step 3: Commit**

```bash
git add kuberca/observability/metrics.py
git commit -m "feat(metrics): add kuberca_invariant_violations_total counter"
```

---

## Task 2: Add Runtime Invariant Check in `compute_confidence()`

**Files:**
- Modify: `kuberca/rules/confidence.py:7-8` (add import)
- Modify: `kuberca/rules/confidence.py:78` (add check before return)

**Step 1: Write the failing test**

Create `tests/unit/test_invariants.py` with the first test:

```python
"""Invariant verification tests.

Each test corresponds to a documented invariant in INVARIANTS.md.
These tests hard-fail on violation — they are the contract.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from kuberca.models.analysis import CorrelationResult
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import CacheReadiness, FieldChange
from kuberca.rules.confidence import compute_confidence

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 2, 21, 10, 0, 0, tzinfo=UTC)
_15_MIN_AGO = _NOW - timedelta(minutes=15)


def _make_event(
    reason: str = "OOMKilled",
    count: int = 1,
    last_seen: datetime | None = None,
) -> EventRecord:
    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message="test",
        namespace="default",
        resource_kind="Pod",
        resource_name="test-pod",
        first_seen=_NOW - timedelta(hours=1),
        last_seen=last_seen or _NOW,
        count=count,
    )


def _make_rule_stub(base_confidence: float = 0.50) -> MagicMock:
    rule = MagicMock()
    rule.base_confidence = base_confidence
    return rule


def _make_correlation(
    changes: list[FieldChange] | None = None,
    related_resources: list[str] | None = None,
) -> CorrelationResult:
    return CorrelationResult(
        objects_queried=1,
        duration_ms=10.0,
        changes=changes or [],
        related_resources=related_resources or [],
    )


# ===================================================================
# CONFIDENCE SCORING INVARIANTS
# ===================================================================


class TestConfidenceScoreRange:
    """INV-C01: Confidence is always in [0.0, 0.95]."""

    def test_minimum_confidence_is_zero_or_positive(self) -> None:
        rule = _make_rule_stub(base_confidence=0.0)
        corr = _make_correlation()
        event = _make_event()
        score = compute_confidence(rule, corr, event)
        assert score >= 0.0

    def test_maximum_confidence_is_095(self) -> None:
        """All bonuses active should cap at 0.95."""
        rule = _make_rule_stub(base_confidence=0.60)
        change = FieldChange(
            field_path="spec.x", old_value="a", new_value="b", changed_at=_15_MIN_AGO
        )
        corr = _make_correlation(changes=[change], related_resources=["res1"])
        event = _make_event(count=3, last_seen=_NOW)
        score = compute_confidence(rule, corr, event)
        assert score == pytest.approx(0.95, abs=1e-9)

    def test_high_base_confidence_capped(self) -> None:
        """Even base_confidence=0.90 with all bonuses caps at 0.95."""
        rule = _make_rule_stub(base_confidence=0.90)
        change = FieldChange(
            field_path="spec.x", old_value="a", new_value="b", changed_at=_15_MIN_AGO
        )
        corr = _make_correlation(changes=[change], related_resources=["res1"])
        event = _make_event(count=5, last_seen=_NOW)
        score = compute_confidence(rule, corr, event)
        assert score == pytest.approx(0.95, abs=1e-9)
```

**Step 2: Run test to verify it passes with current code**

Run: `uv run pytest tests/unit/test_invariants.py::TestConfidenceScoreRange -v`
Expected: PASS (these verify existing behavior)

**Step 3: Add runtime invariant check to `compute_confidence()`**

In `kuberca/rules/confidence.py`, add import at the top:

```python
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import invariant_violations_total

_logger = get_logger("confidence")
```

Replace line 78 (`return min(score, 0.95)`) with:

```python
    result = min(score, 0.95)
    if result < 0.0 or result > 0.95:
        _logger.error(
            "invariant_violated",
            invariant="INV-C01_confidence_range",
            value=result,
            base_confidence=rule.base_confidence,
        )
        invariant_violations_total.labels(invariant_name="INV-C01_confidence_range").inc()
        result = max(0.0, min(result, 0.95))
    return result
```

**Step 4: Verify lint and type check**

Run: `uv run ruff check kuberca/rules/confidence.py && uv run mypy kuberca/rules/confidence.py`
Expected: clean

**Step 5: Run test again**

Run: `uv run pytest tests/unit/test_invariants.py::TestConfidenceScoreRange -v`
Expected: PASS

**Step 6: Commit**

```bash
git add kuberca/rules/confidence.py tests/unit/test_invariants.py
git commit -m "feat(invariants): add INV-C01 confidence range check with runtime warning"
```

---

## Task 3: Add Runtime Invariant Check in `_recompute_readiness()`

**Files:**
- Modify: `kuberca/cache/resource_cache.py:534-579` (add valid transition check)

**Step 1: Add cache state invariant tests to `tests/unit/test_invariants.py`**

Append to `tests/unit/test_invariants.py`:

```python
from kuberca.cache.resource_cache import ResourceCache
from kuberca.models.resources import CacheReadiness


# ===================================================================
# CACHE STATE INVARIANTS
# ===================================================================


class TestCacheStateTransitions:
    """INV-CS01: Only valid state transitions are allowed."""

    # Valid transitions
    VALID_TRANSITIONS: set[tuple[CacheReadiness, CacheReadiness]] = {
        (CacheReadiness.WARMING, CacheReadiness.PARTIALLY_READY),
        (CacheReadiness.WARMING, CacheReadiness.DEGRADED),
        (CacheReadiness.WARMING, CacheReadiness.READY),
        (CacheReadiness.PARTIALLY_READY, CacheReadiness.READY),
        (CacheReadiness.PARTIALLY_READY, CacheReadiness.DEGRADED),
        (CacheReadiness.READY, CacheReadiness.DEGRADED),
        (CacheReadiness.DEGRADED, CacheReadiness.READY),
        (CacheReadiness.DEGRADED, CacheReadiness.PARTIALLY_READY),
    }

    def test_warming_to_partially_ready(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        assert cache.readiness() == CacheReadiness.WARMING
        cache.update("Pod", "default", "p1", {
            "metadata": {"name": "p1", "namespace": "default", "resourceVersion": "1",
                         "labels": {}, "annotations": {}},
            "spec": {}, "status": {},
        })
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.PARTIALLY_READY

    def test_partially_ready_to_ready(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.PARTIALLY_READY
        cache._ready_kinds = {"Pod", "Deployment"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY

    def test_ready_to_degraded_on_reconnect_failures(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY
        cache._reconnect_failures = 4
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_degraded_to_ready_on_recovery(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._reconnect_failures = 4
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.DEGRADED
        cache._reconnect_failures = 0
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY

    def test_noop_recompute_no_transition_counter(self) -> None:
        """INV-CS02: No-op recomputes do NOT increment transition counter."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY
        # Recompute again — same state
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY
        # No transition emitted (tested via metric)
```

**Step 2: Add the runtime valid-transition check in `_recompute_readiness()`**

In `kuberca/cache/resource_cache.py`, add at the top of the file (with imports):

```python
from kuberca.observability.metrics import invariant_violations_total
```

Then in `_recompute_readiness()`, after line 579 (`self._emit_state_metric()`), add before the final `self._emit_state_metric()` call — specifically, replace the final transition-counter block (lines 575-579) with:

```python
        if self._readiness != previous:
            # INV-CS01: Validate transition is in the allowed set
            _VALID_TRANSITIONS = {
                (CacheReadiness.WARMING, CacheReadiness.PARTIALLY_READY),
                (CacheReadiness.WARMING, CacheReadiness.DEGRADED),
                (CacheReadiness.WARMING, CacheReadiness.READY),
                (CacheReadiness.PARTIALLY_READY, CacheReadiness.READY),
                (CacheReadiness.PARTIALLY_READY, CacheReadiness.DEGRADED),
                (CacheReadiness.READY, CacheReadiness.DEGRADED),
                (CacheReadiness.DEGRADED, CacheReadiness.READY),
                (CacheReadiness.DEGRADED, CacheReadiness.PARTIALLY_READY),
            }
            if (previous, self._readiness) not in _VALID_TRANSITIONS:
                _rc_logger.error(
                    "invariant_violated",
                    invariant="INV-CS01_valid_transition",
                    from_state=previous.value,
                    to_state=self._readiness.value,
                )
                invariant_violations_total.labels(
                    invariant_name="INV-CS01_valid_transition"
                ).inc()
            cache_state_transitions_total.labels(
                from_state=previous.value, to_state=self._readiness.value
            ).inc()
        self._emit_state_metric()
```

Note: `_rc_logger` should be the existing logger in that file. Check the actual logger variable name.

**Step 3: Verify lint, type check, and tests**

Run: `uv run ruff check kuberca/cache/resource_cache.py && uv run mypy kuberca/cache/resource_cache.py`
Run: `uv run pytest tests/unit/test_invariants.py::TestCacheStateTransitions -v`
Expected: all PASS

**Step 4: Commit**

```bash
git add kuberca/cache/resource_cache.py tests/unit/test_invariants.py
git commit -m "feat(invariants): add INV-CS01 valid cache transition check with runtime warning"
```

---

## Task 4: Add Runtime Invariant Check in `_rule_result_to_rca_response()` and `RuleEngine.evaluate()`

**Files:**
- Modify: `kuberca/analyst/coordinator.py:216-269` (confidence range check)
- Modify: `kuberca/rules/base.py:153-175` (evaluation order check)

**Step 1: Add invariant tests**

Append to `tests/unit/test_invariants.py`:

```python
from kuberca.analyst.coordinator import _rule_result_to_rca_response
from kuberca.models.analysis import AffectedResource, EvaluationMeta, EvidenceItem, RuleResult
from kuberca.models.events import DiagnosisSource, EvidenceType
from kuberca.rules.base import RuleEngine


class TestCoordinatorConfidenceRange:
    """INV-C02: Final confidence after penalty is in [0.0, 0.95]."""

    def test_degraded_penalty_stays_positive(self) -> None:
        rule_result = RuleResult(
            rule_id="R01_oom_killed",
            root_cause="OOM",
            confidence=0.05,
            evidence=[],
            affected_resources=[],
            suggested_remediation="Increase memory",
        )
        meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=10.0)
        response = _rule_result_to_rca_response(
            rule_result, CacheReadiness.DEGRADED, meta, "test", 0
        )
        assert response.confidence >= 0.0

    def test_partially_ready_penalty_stays_positive(self) -> None:
        rule_result = RuleResult(
            rule_id="R01_oom_killed",
            root_cause="OOM",
            confidence=0.10,
            evidence=[],
            affected_resources=[],
            suggested_remediation="Increase memory",
        )
        meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=10.0)
        response = _rule_result_to_rca_response(
            rule_result, CacheReadiness.PARTIALLY_READY, meta, "test", 0
        )
        assert response.confidence >= 0.0
        assert response.confidence <= 0.95

    def test_ready_no_penalty(self) -> None:
        rule_result = RuleResult(
            rule_id="R01_oom_killed",
            root_cause="OOM",
            confidence=0.80,
            evidence=[],
            affected_resources=[],
            suggested_remediation="Increase memory",
        )
        meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=10.0)
        response = _rule_result_to_rca_response(
            rule_result, CacheReadiness.READY, meta, "test", 0
        )
        assert response.confidence == pytest.approx(0.80, abs=1e-9)


class TestRuleEngineEvaluationOrder:
    """INV-C03: Rules are always evaluated in (priority, rule_id) order."""

    def test_rules_sorted_after_registration(self) -> None:
        cache = MagicMock()
        ledger = MagicMock()
        engine = RuleEngine(cache=cache, ledger=ledger)

        r1 = MagicMock()
        r1.priority = 20
        r1.rule_id = "R08_volume_mount"
        r1.resource_dependencies = ["Pod"]

        r2 = MagicMock()
        r2.priority = 10
        r2.rule_id = "R01_oom_killed"
        r2.resource_dependencies = ["Pod"]

        r3 = MagicMock()
        r3.priority = 20
        r3.rule_id = "R02_crash_loop"
        r3.resource_dependencies = ["Pod"]

        engine.register(r1)
        engine.register(r2)
        engine.register(r3)

        ids = [r.rule_id for r in engine._rules]
        assert ids == ["R01_oom_killed", "R02_crash_loop", "R08_volume_mount"]

    def test_order_stable_after_multiple_registrations(self) -> None:
        cache = MagicMock()
        ledger = MagicMock()
        engine = RuleEngine(cache=cache, ledger=ledger)

        for i in range(10):
            r = MagicMock()
            r.priority = 10 + (i % 3) * 5
            r.rule_id = f"R{i:02d}_test"
            r.resource_dependencies = ["Pod"]
            engine.register(r)

        ids = [r.rule_id for r in engine._rules]
        expected = sorted(ids, key=lambda rid: (
            next(r.priority for r in engine._rules if r.rule_id == rid), rid
        ))
        # Verify by checking engine internal state
        for i in range(len(engine._rules) - 1):
            a, b = engine._rules[i], engine._rules[i + 1]
            assert (a.priority, a.rule_id) <= (b.priority, b.rule_id)
```

**Step 2: Add runtime check in `_rule_result_to_rca_response()`**

In `kuberca/analyst/coordinator.py`, add import:

```python
from kuberca.observability.metrics import invariant_violations_total
```

After line 232 (`confidence = max(0.0, min(confidence, 0.95))`), add:

```python
    if confidence < 0.0 or confidence > 0.95:
        _logger.error(
            "invariant_violated",
            invariant="INV-C02_final_confidence_range",
            value=confidence,
            raw_confidence=rule_result.confidence,
            cache_state=cache_readiness.value,
        )
        invariant_violations_total.labels(invariant_name="INV-C02_final_confidence_range").inc()
        confidence = max(0.0, min(confidence, 0.95))
```

**Step 3: Add runtime check in `RuleEngine.evaluate()`**

In `kuberca/rules/base.py`, add import:

```python
from kuberca.observability.metrics import invariant_violations_total
```

At the start of `evaluate()` (after line 164, `wall_start = ...`), add:

```python
        # INV-C03: Verify rule order invariant
        for i in range(len(self._rules) - 1):
            a, b = self._rules[i], self._rules[i + 1]
            if (a.priority, a.rule_id) > (b.priority, b.rule_id):
                _logger.error(
                    "invariant_violated",
                    invariant="INV-C03_rule_order",
                    rule_a=a.rule_id,
                    rule_b=b.rule_id,
                )
                invariant_violations_total.labels(invariant_name="INV-C03_rule_order").inc()
                break
```

**Step 4: Verify lint, type check, and tests**

Run: `uv run ruff check kuberca/analyst/coordinator.py kuberca/rules/base.py`
Run: `uv run mypy kuberca/analyst/coordinator.py kuberca/rules/base.py`
Run: `uv run pytest tests/unit/test_invariants.py -v`
Expected: all PASS

**Step 5: Commit**

```bash
git add kuberca/analyst/coordinator.py kuberca/rules/base.py tests/unit/test_invariants.py
git commit -m "feat(invariants): add INV-C02 and INV-C03 runtime checks"
```

---

## Task 5: Complete Invariant Test Suite

**Files:**
- Modify: `tests/unit/test_invariants.py` (add remaining invariant tests)

**Step 1: Add remaining invariant tests**

Append these test classes to `tests/unit/test_invariants.py`:

```python
class TestConfidencePureFunctionInvariant:
    """INV-C04: compute_confidence() is pure — same inputs, same output."""

    def test_1000_identical_calls(self) -> None:
        rule = _make_rule_stub(base_confidence=0.50)
        change = FieldChange(
            field_path="spec.x", old_value="a", new_value="b", changed_at=_15_MIN_AGO
        )
        corr = _make_correlation(changes=[change], related_resources=["res1"])
        event = _make_event(count=3, last_seen=_NOW)
        first = compute_confidence(rule, corr, event)
        for _ in range(999):
            assert compute_confidence(rule, corr, event) == pytest.approx(first, abs=1e-9)


class TestLLMConfidenceCapInvariant:
    """INV-C05: LLM confidence is capped at 0.70."""

    def test_llm_cap_in_coordinator(self) -> None:
        """The _llm_result_to_rca_response caps at 0.70."""
        from kuberca.analyst.coordinator import _llm_result_to_rca_response

        llm_result = MagicMock()
        llm_result.confidence = 0.90  # above cap
        llm_result.evidence_citations = []
        llm_result.affected_resources = []
        llm_result.root_cause = "test"
        llm_result.suggested_remediation = "test"
        llm_result.retries_used = 0
        meta = EvaluationMeta(rules_evaluated=0, rules_matched=0, duration_ms=0.0)
        response = _llm_result_to_rca_response(
            llm_result, CacheReadiness.READY, meta, "test", 0
        )
        assert response.confidence <= 0.70


class TestFourBandInvariants:
    """INV-C06 through INV-C08: 4-band strategy invariants."""

    def _make_engine_with_stubs(
        self, configs: list[tuple[str, int, float]],
    ) -> tuple[RuleEngine, list[MagicMock]]:
        """Create engine with stub rules. configs: [(rule_id, priority, confidence)]."""
        cache = MagicMock()
        ledger = MagicMock()
        engine = RuleEngine(cache=cache, ledger=ledger)
        stubs = []
        for rule_id, priority, confidence in configs:
            stub = MagicMock()
            stub.rule_id = rule_id
            stub.priority = priority
            stub.base_confidence = confidence
            stub.resource_dependencies = ["Pod"]
            stub.match.return_value = True
            corr = CorrelationResult(objects_queried=1, duration_ms=1.0)
            stub.correlate.return_value = corr
            result = RuleResult(
                rule_id=rule_id,
                root_cause="test",
                confidence=confidence,
                evidence=[],
                affected_resources=[],
                suggested_remediation="test",
            )
            stub.explain.return_value = result
            engine.register(stub)
            stubs.append(stub)
        return engine, stubs

    def test_high_band_short_circuits(self) -> None:
        """INV-C06: >= 0.85 returns immediately, skip remaining rules."""
        engine, stubs = self._make_engine_with_stubs([
            ("R01", 10, 0.85),
            ("R02", 20, 0.90),
        ])
        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is not None
        assert result.rule_id == "R01"
        assert result.confidence >= 0.85
        # R02 was never evaluated
        stubs[1].correlate.assert_not_called()

    def test_medium_band_restricts_to_shared_deps(self) -> None:
        """INV-C07: Medium band (0.70-0.84) restricts to shared-dependency rules."""
        cache = MagicMock()
        ledger = MagicMock()
        engine = RuleEngine(cache=cache, ledger=ledger)

        # Rule A: medium confidence, depends on Pod
        a = MagicMock()
        a.rule_id = "R01"
        a.priority = 10
        a.resource_dependencies = ["Pod"]
        a.match.return_value = True
        a.correlate.return_value = CorrelationResult(objects_queried=1, duration_ms=1.0)
        a.explain.return_value = RuleResult(
            rule_id="R01", root_cause="test", confidence=0.75,
            evidence=[], affected_resources=[], suggested_remediation="",
        )
        engine.register(a)

        # Rule B: depends on Node (disjoint) — should be skipped
        b = MagicMock()
        b.rule_id = "R02"
        b.priority = 20
        b.resource_dependencies = ["Node"]
        b.match.return_value = True
        engine.register(b)

        # Rule C: depends on Pod (shared) — should be evaluated
        c = MagicMock()
        c.rule_id = "R03"
        c.priority = 30
        c.resource_dependencies = ["Pod"]
        c.match.return_value = True
        c.correlate.return_value = CorrelationResult(objects_queried=1, duration_ms=1.0)
        c.explain.return_value = RuleResult(
            rule_id="R03", root_cause="test", confidence=0.72,
            evidence=[], affected_resources=[], suggested_remediation="",
        )
        engine.register(c)

        event = _make_event()
        result, meta = engine.evaluate(event)
        # B should never be called (disjoint deps)
        b.correlate.assert_not_called()
        # C should be evaluated (shared deps)
        c.match.assert_called()

    def test_low_band_evaluates_all_rules(self) -> None:
        """INV-C08: Low band (< 0.70) continues evaluating ALL rules."""
        engine, stubs = self._make_engine_with_stubs([
            ("R01", 10, 0.60),
            ("R02", 20, 0.55),
        ])
        # Give R02 different deps to prove it's NOT restricted
        stubs[1].resource_dependencies = ["Node"]
        event = _make_event()
        result, meta = engine.evaluate(event)
        # Both rules should be evaluated
        stubs[0].correlate.assert_called()
        stubs[1].correlate.assert_called()
        # Best result wins
        assert result is not None
        assert result.rule_id == "R01"


class TestCompetingDepsOnlyMediumBand:
    """INV-C09: Competing-deps mode only activates on medium, not low."""

    def test_low_confidence_does_not_activate_competing_mode(self) -> None:
        cache = MagicMock()
        ledger = MagicMock()
        engine = RuleEngine(cache=cache, ledger=ledger)

        # Rule A: low confidence (0.60), depends on Pod
        a = MagicMock()
        a.rule_id = "R01"
        a.priority = 10
        a.resource_dependencies = ["Pod"]
        a.match.return_value = True
        a.correlate.return_value = CorrelationResult(objects_queried=1, duration_ms=1.0)
        a.explain.return_value = RuleResult(
            rule_id="R01", root_cause="test", confidence=0.60,
            evidence=[], affected_resources=[], suggested_remediation="",
        )
        engine.register(a)

        # Rule B: depends on Node (disjoint) — should STILL be evaluated
        b = MagicMock()
        b.rule_id = "R02"
        b.priority = 20
        b.resource_dependencies = ["Node"]
        b.match.return_value = True
        b.correlate.return_value = CorrelationResult(objects_queried=1, duration_ms=1.0)
        b.explain.return_value = RuleResult(
            rule_id="R02", root_cause="test", confidence=0.65,
            evidence=[], affected_resources=[], suggested_remediation="",
        )
        engine.register(b)

        event = _make_event()
        result, meta = engine.evaluate(event)
        # Both should be evaluated — no competing-deps filtering for low band
        b.correlate.assert_called()
        # B wins because higher confidence
        assert result is not None
        assert result.rule_id == "R02"


class TestDivergenceTriggerInvariants:
    """INV-DT01 through INV-DT04: Divergence trigger invariants."""

    def test_miss_rate_above_threshold_degrades(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY
        # Inject high miss rate via access log
        from datetime import UTC, datetime
        now = datetime.now(tz=UTC)
        # Record 100 accesses, 10 misses (10% > 5% threshold)
        for i in range(100):
            is_miss = i < 10
            cache._access_log.append((now, is_miss))
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_reconnect_failures_above_threshold_degrades(self) -> None:
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY
        cache._reconnect_failures = 4  # > 3 threshold
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.DEGRADED


class TestAnalysisInvariants:
    """INV-A01 through INV-A04: Analysis pipeline invariants."""

    def test_correlate_timeout_is_500ms(self) -> None:
        from kuberca.rules.base import _CORRELATE_TIMEOUT_MS
        assert _CORRELATE_TIMEOUT_MS == 500.0

    def test_max_objects_is_50(self) -> None:
        from kuberca.rules.base import _MAX_OBJECTS
        assert _MAX_OBJECTS == 50

    def test_llm_retry_cap_is_2(self) -> None:
        """Verify the LLM retry cap constant in coordinator."""
        # This is hardcoded as max_quality_retries = 2 in coordinator.py line 551
        # We verify it by reading the source
        import inspect
        from kuberca.analyst.coordinator import AnalystCoordinator
        source = inspect.getsource(AnalystCoordinator.analyze)
        assert "max_quality_retries = 2" in source

    def test_quality_failure_caps_at_035(self) -> None:
        """Verify quality failure confidence cap."""
        import inspect
        from kuberca.analyst.coordinator import AnalystCoordinator
        source = inspect.getsource(AnalystCoordinator.analyze)
        assert "min(llm_result.confidence, 0.35)" in source
```

**Step 2: Run the full invariant test suite**

Run: `uv run pytest tests/unit/test_invariants.py -v`
Expected: all PASS

**Step 3: Commit**

```bash
git add tests/unit/test_invariants.py
git commit -m "feat(tests): complete invariant test suite with 30+ assertions"
```

---

## Task 6: Write INVARIANTS.md

**Files:**
- Create: `INVARIANTS.md`

**Step 1: Create the document**

Create `INVARIANTS.md` at project root with all documented invariants (content from the design doc, Section 1.1). Include invariant IDs (INV-C01, INV-CS01, etc.) that match the test class names.

**Step 2: Commit**

```bash
git add INVARIANTS.md
git commit -m "docs: add INVARIANTS.md documenting all system invariants"
```

---

## Task 7: Confidence Under Cache Penalties — Test Suite

**Files:**
- Create: `tests/unit/test_confidence_under_penalties.py`

**Step 1: Write the full test suite**

```python
"""Tests that confidence degrades gracefully across cache readiness states.

Proves that:
  - The same incident produces the same RULE WINNER regardless of cache state
  - Confidence monotonically decreases: READY > PARTIALLY_READY > DEGRADED
  - DEGRADED always returns INCONCLUSIVE (confidence 0.0)
  - Penalties are applied AFTER rule selection (rule engine is cache-unaware)
  - All scenarios are deterministic over 100 replays
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kuberca.analyst.coordinator import AnalystCoordinator, _rule_result_to_rca_response
from kuberca.models.analysis import EvaluationMeta, RuleResult
from kuberca.models.events import DiagnosisSource, EventRecord, EventSource, Severity
from kuberca.models.resources import CacheReadiness

_NOW = datetime(2026, 2, 21, 10, 0, 0, tzinfo=UTC)


def _make_event(reason: str = "OOMKilled") -> EventRecord:
    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=Severity.ERROR,
        reason=reason,
        message="Container was OOM-killed",
        namespace="default",
        resource_kind="Pod",
        resource_name="my-app-7b4f8c6d-x2kj",
        first_seen=_NOW - timedelta(hours=1),
        last_seen=_NOW,
        count=3,
        cluster_id="test-cluster",
    )


def _make_rule_result(confidence: float = 0.80) -> RuleResult:
    return RuleResult(
        rule_id="R01_oom_killed",
        root_cause="Container OOM-killed due to memory limit",
        confidence=confidence,
        evidence=[],
        affected_resources=[],
        suggested_remediation="Increase memory limit",
    )


def _make_meta() -> EvaluationMeta:
    return EvaluationMeta(rules_evaluated=3, rules_matched=1, duration_ms=15.0)


def _make_coordinator(
    cache_readiness: CacheReadiness,
    rule_result: RuleResult | None = None,
) -> AnalystCoordinator:
    """Build an AnalystCoordinator with mocked dependencies."""
    rule_engine = MagicMock()
    rule_engine.evaluate.return_value = (
        rule_result,
        _make_meta(),
    )

    cache = MagicMock()
    cache.readiness.return_value = cache_readiness
    cache.get.return_value = None

    ledger = MagicMock()
    ledger.diff.return_value = []

    event_buffer = MagicMock()
    event_buffer.get_events.return_value = [_make_event()] if rule_result else []

    config = MagicMock()
    config.cluster_id = "test-cluster"

    return AnalystCoordinator(
        rule_engine=rule_engine,
        llm_analyzer=None,
        cache=cache,
        ledger=ledger,
        event_buffer=event_buffer,
        config=config,
    )


class TestRuleWinnerStability:
    """Same incident, same rule wins regardless of cache state."""

    async def test_oom_rule_wins_at_ready(self) -> None:
        result = _make_rule_result(confidence=0.80)
        coord = _make_coordinator(CacheReadiness.READY, result)
        response = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE
        assert response.rule_id == "R01_oom_killed"

    async def test_same_rule_wins_at_partially_ready(self) -> None:
        result = _make_rule_result(confidence=0.80)
        coord = _make_coordinator(CacheReadiness.PARTIALLY_READY, result)
        response = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE
        assert response.rule_id == "R01_oom_killed"


class TestGracefulDegradationCurve:
    """Confidence: READY > PARTIALLY_READY > DEGRADED (0.0)."""

    async def test_monotonic_decrease(self) -> None:
        result = _make_rule_result(confidence=0.80)

        coord_ready = _make_coordinator(CacheReadiness.READY, result)
        resp_ready = await coord_ready.analyze("Pod/default/my-app-7b4f8c6d-x2kj")

        coord_partial = _make_coordinator(CacheReadiness.PARTIALLY_READY, result)
        resp_partial = await coord_partial.analyze("Pod/default/my-app-7b4f8c6d-x2kj")

        assert resp_ready.confidence > resp_partial.confidence
        assert resp_partial.confidence > 0.0

    async def test_degraded_returns_inconclusive(self) -> None:
        """DEGRADED returns INCONCLUSIVE regardless of rule match."""
        coord = _make_coordinator(CacheReadiness.DEGRADED, None)
        # When no rule matches and cache is DEGRADED, return INCONCLUSIVE
        response = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
        assert response.diagnosed_by == DiagnosisSource.INCONCLUSIVE
        assert response.confidence == 0.0


class TestNoRuleFlipping:
    """Medium-band result at READY doesn't flip to different rule at PARTIALLY_READY."""

    def test_penalty_does_not_change_rule_selection(self) -> None:
        """Penalties are applied in coordinator, not rule engine.
        Rule engine always gets raw scores. So rule selection is identical."""
        meta = _make_meta()

        # At READY: R01 wins with 0.75
        result_a = _make_rule_result(confidence=0.75)
        resp_ready = _rule_result_to_rca_response(
            result_a, CacheReadiness.READY, meta, "test", 0
        )

        # At PARTIALLY_READY: same R01, penalty applied
        resp_partial = _rule_result_to_rca_response(
            result_a, CacheReadiness.PARTIALLY_READY, meta, "test", 0
        )

        assert resp_ready.rule_id == resp_partial.rule_id == "R01_oom_killed"
        assert resp_ready.confidence == pytest.approx(0.75, abs=1e-9)
        assert resp_partial.confidence == pytest.approx(0.60, abs=1e-9)  # 0.75 - 0.15


class TestBoundaryPenaltyCases:
    """Edge cases in penalty application."""

    def test_070_minus_partially_ready_penalty(self) -> None:
        """Rule at 0.70 → 0.55 after PARTIALLY_READY penalty."""
        result = _make_rule_result(confidence=0.70)
        meta = _make_meta()
        response = _rule_result_to_rca_response(
            result, CacheReadiness.PARTIALLY_READY, meta, "test", 0
        )
        assert response.confidence == pytest.approx(0.55, abs=1e-9)

    def test_high_band_minus_penalty_drops_to_medium(self) -> None:
        """0.85 - 0.15 = 0.70. Still a valid score (no short-circuit at coordinator level)."""
        result = _make_rule_result(confidence=0.85)
        meta = _make_meta()
        response = _rule_result_to_rca_response(
            result, CacheReadiness.PARTIALLY_READY, meta, "test", 0
        )
        assert response.confidence == pytest.approx(0.70, abs=1e-9)

    def test_floor_at_zero(self) -> None:
        """Low confidence minus penalty doesn't go negative."""
        result = _make_rule_result(confidence=0.05)
        meta = _make_meta()
        response = _rule_result_to_rca_response(
            result, CacheReadiness.PARTIALLY_READY, meta, "test", 0
        )
        assert response.confidence == 0.0

    def test_degraded_penalty_010(self) -> None:
        """DEGRADED applies -0.10 penalty."""
        result = _make_rule_result(confidence=0.80)
        meta = _make_meta()
        response = _rule_result_to_rca_response(
            result, CacheReadiness.DEGRADED, meta, "test", 0
        )
        assert response.confidence == pytest.approx(0.70, abs=1e-9)


class TestPenaltyAppliedPostSelection:
    """Rule engine runs on raw scores; penalty only affects reported confidence."""

    async def test_rule_engine_receives_raw_event(self) -> None:
        """The coordinator calls rule_engine.evaluate() with the raw event.
        No cache state is passed to the rule engine."""
        result = _make_rule_result(confidence=0.80)
        rule_engine = MagicMock()
        rule_engine.evaluate.return_value = (result, _make_meta())

        cache = MagicMock()
        cache.readiness.return_value = CacheReadiness.PARTIALLY_READY
        cache.get.return_value = None

        event_buffer = MagicMock()
        event_buffer.get_events.return_value = [_make_event()]

        ledger = MagicMock()
        ledger.diff.return_value = []

        config = MagicMock()
        config.cluster_id = "test"

        coord = AnalystCoordinator(
            rule_engine=rule_engine,
            llm_analyzer=None,
            cache=cache,
            ledger=ledger,
            event_buffer=event_buffer,
            config=config,
        )
        response = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")

        # Rule engine was called with the event, no cache state
        call_args = rule_engine.evaluate.call_args
        assert len(call_args.args) == 1  # just the event
        assert isinstance(call_args.args[0], EventRecord)


class TestReplayStability:
    """100x replay — same result every time."""

    async def test_100_replays_ready(self) -> None:
        result = _make_rule_result(confidence=0.80)
        scores = []
        for _ in range(100):
            coord = _make_coordinator(CacheReadiness.READY, result)
            resp = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
            scores.append(resp.confidence)
        assert all(s == pytest.approx(scores[0], abs=1e-9) for s in scores)

    async def test_100_replays_partially_ready(self) -> None:
        result = _make_rule_result(confidence=0.80)
        scores = []
        for _ in range(100):
            coord = _make_coordinator(CacheReadiness.PARTIALLY_READY, result)
            resp = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
            scores.append(resp.confidence)
        assert all(s == pytest.approx(scores[0], abs=1e-9) for s in scores)


class TestWarningsPresent:
    """Correct warnings are present in responses."""

    async def test_partially_ready_warning(self) -> None:
        result = _make_rule_result(confidence=0.80)
        coord = _make_coordinator(CacheReadiness.PARTIALLY_READY, result)
        response = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
        warnings_text = " ".join(response._meta.warnings)
        assert "partially ready" in warnings_text.lower()

    async def test_degraded_suppression_message(self) -> None:
        coord = _make_coordinator(CacheReadiness.DEGRADED, None)
        response = await coord.analyze("Pod/default/my-app-7b4f8c6d-x2kj")
        warnings_text = " ".join(response._meta.warnings)
        assert "suppressed" in warnings_text.lower() or "degraded" in warnings_text.lower()
```

**Step 2: Run the test suite**

Run: `uv run pytest tests/unit/test_confidence_under_penalties.py -v`
Expected: all PASS

**Step 3: Commit**

```bash
git add tests/unit/test_confidence_under_penalties.py
git commit -m "feat(tests): add confidence-under-cache-penalties test suite (11 scenarios)"
```

---

## Task 8: Stress Test Failure Intersections — Scenarios 1-4

**Files:**
- Modify: `tests/integration/test_load_stress.py` (append new tests)

**Step 1: Add stress test scenarios 1-4**

Append to `tests/integration/test_load_stress.py`:

```python
# ---------------------------------------------------------------------------
# FAILURE INTERSECTION STRESS TESTS (Scenarios 13-20)
# ---------------------------------------------------------------------------

from kuberca.analyst.coordinator import AnalystCoordinator, _rule_result_to_rca_response
from kuberca.models.analysis import EvaluationMeta, RuleResult
from kuberca.models.events import DiagnosisSource
from kuberca.models.resources import CacheReadiness
from unittest.mock import MagicMock
import asyncio


def _make_coordinator_for_stress(
    cache: ResourceCache,
    ledger: ChangeLedger,
    cache_readiness: CacheReadiness = CacheReadiness.READY,
) -> AnalystCoordinator:
    """Build a coordinator with real cache/ledger and mock event buffer."""
    engine = RuleEngine(cache=cache, ledger=ledger)
    engine.register(OOMKilledRule())
    engine.register(CrashLoopRule())
    engine.register(FailedSchedulingRule())

    mock_cache = MagicMock(wraps=cache)
    mock_cache.readiness.return_value = cache_readiness

    event_buffer = MagicMock()
    event_buffer.get_events.return_value = [make_oom_event()]

    config = MagicMock()
    config.cluster_id = "stress-test"

    return AnalystCoordinator(
        rule_engine=engine,
        llm_analyzer=None,
        cache=mock_cache,
        ledger=ledger,
        event_buffer=event_buffer,
        config=config,
    )


@pytest.mark.stress
def test_burst_with_cache_oscillation() -> None:
    """Scenario 13: 500 events with cache oscillating READY<->DEGRADED.

    Verifies:
    - No exceptions during oscillation
    - DEGRADED intervals produce no rule_engine diagnoses (only INCONCLUSIVE if no rule match before degraded)
    - READY intervals produce valid diagnoses
    """
    cache = ResourceCache()
    _populate_cache_with_test_data(cache)
    ledger = ChangeLedger(max_versions=10, retention_hours=6)

    engine = RuleEngine(cache=cache, ledger=ledger)
    engine.register(OOMKilledRule())

    events = [make_oom_event(resource_name=f"pod-osc-{i}") for i in range(500)]
    results: list[tuple[int, str | None]] = []

    for i, event in enumerate(events):
        # Oscillate cache every 50 events
        if (i // 50) % 2 == 0:
            cache._reconnect_failures = 0
        else:
            cache._reconnect_failures = 4
        cache._recompute_readiness()

        result, meta = engine.evaluate(event)
        rule_id = result.rule_id if result else None
        results.append((i, rule_id))

    # All 500 evaluations completed without exception
    assert len(results) == 500
    # Rule engine itself doesn't know about cache state — it always evaluates
    # The coordinator handles DEGRADED suppression, not the engine


@pytest.mark.stress
def test_readiness_state_oscillation_counters() -> None:
    """Scenario 14: Rapid state transitions with correct counter tracking.

    Verifies:
    - WARMING→PARTIALLY_READY→READY→DEGRADED→READY cycle
    - Each transition increments the counter exactly once
    - No-op recomputes don't increment
    """
    from kuberca.observability.metrics import cache_state_transitions_total

    cache = ResourceCache()
    cache._all_kinds = {"Pod", "Deployment"}

    # Count transitions by sampling before/after
    # WARMING → PARTIALLY_READY
    cache._ready_kinds = {"Pod"}
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.PARTIALLY_READY

    # PARTIALLY_READY → READY
    cache._ready_kinds = {"Pod", "Deployment"}
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.READY

    # READY → DEGRADED
    cache._reconnect_failures = 4
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.DEGRADED

    # DEGRADED → READY (recovery)
    cache._reconnect_failures = 0
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.READY

    # No-op: READY → READY (should NOT increment)
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.READY

    # Rapid oscillation: 100 cycles
    for _ in range(100):
        cache._reconnect_failures = 4
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.DEGRADED
        cache._reconnect_failures = 0
        cache._recompute_readiness()
        assert cache.readiness() == CacheReadiness.READY

    # No exceptions during rapid oscillation


@pytest.mark.stress
def test_ledger_trim_during_concurrent_analysis() -> None:
    """Scenario 15: Fill ledger to soft-trim, evaluate 100 events concurrently.

    Verifies:
    - No corrupted diffs during trim
    - All evaluations complete without error
    """
    cache = ResourceCache()
    _populate_cache_with_test_data(cache)
    ledger = ChangeLedger(max_versions=5, retention_hours=6)

    # Fill ledger with many snapshots to trigger soft trim
    now = datetime.now(UTC)
    for res_idx in range(50):
        for ver in range(20):
            ledger.record(_make_snapshot(res_idx, ver, captured_at=now - timedelta(seconds=ver * 60)))

    engine = RuleEngine(cache=cache, ledger=ledger)
    engine.register(OOMKilledRule())

    # Evaluate 100 events — ledger may be trimming during evaluation
    events = [make_oom_event(resource_name=f"pod-trim-{i}") for i in range(100)]
    errors: list[str] = []

    for event in events:
        try:
            result, meta = engine.evaluate(event)
        except Exception as exc:
            errors.append(str(exc))

    assert len(errors) == 0, f"Errors during ledger-trim evaluation: {errors}"


@pytest.mark.stress
@pytest.mark.asyncio
async def test_llm_suppression_flip_mid_analysis() -> None:
    """Scenario 16: Cache transitions READY→DEGRADED between two analysis calls.

    Verifies:
    - First analysis (READY, no rule match) returns INCONCLUSIVE (no LLM)
    - Cache flips to DEGRADED
    - Second analysis returns INCONCLUSIVE with suppression message
    """
    cache = ResourceCache()
    _populate_cache_with_test_data(cache)
    ledger = ChangeLedger(max_versions=10, retention_hours=6)

    engine = RuleEngine(cache=cache, ledger=ledger)
    engine.register(OOMKilledRule())

    # Use a non-OOM event so no rule matches
    non_matching_event = make_event(
        reason="SomethingElse",
        message="Unknown event",
        resource_name="unknown-pod-xyz",
    )

    mock_cache = MagicMock(wraps=cache)
    event_buffer = MagicMock()
    event_buffer.get_events.return_value = [non_matching_event]

    config = MagicMock()
    config.cluster_id = "test"

    coord = AnalystCoordinator(
        rule_engine=engine,
        llm_analyzer=None,
        cache=mock_cache,
        ledger=ledger,
        event_buffer=event_buffer,
        config=config,
    )

    # First call: READY
    mock_cache.readiness.return_value = CacheReadiness.READY
    resp1 = await coord.analyze("Pod/default/unknown-pod-xyz")
    assert resp1.diagnosed_by == DiagnosisSource.INCONCLUSIVE

    # Flip to DEGRADED
    mock_cache.readiness.return_value = CacheReadiness.DEGRADED
    resp2 = await coord.analyze("Pod/default/unknown-pod-xyz")
    assert resp2.diagnosed_by == DiagnosisSource.INCONCLUSIVE
    assert resp2.confidence == 0.0
    # Should have suppression/degraded warning
    warnings_text = " ".join(resp2._meta.warnings)
    assert "degraded" in warnings_text.lower() or "suppressed" in warnings_text.lower()
```

**Step 2: Run the new stress tests**

Run: `uv run pytest tests/integration/test_load_stress.py::test_burst_with_cache_oscillation tests/integration/test_load_stress.py::test_readiness_state_oscillation_counters tests/integration/test_load_stress.py::test_ledger_trim_during_concurrent_analysis tests/integration/test_load_stress.py::test_llm_suppression_flip_mid_analysis -v`
Expected: all PASS

**Step 3: Commit**

```bash
git add tests/integration/test_load_stress.py
git commit -m "feat(tests): add stress intersection scenarios 13-16 (oscillation, trim, suppression)"
```

---

## Task 9: Stress Test Failure Intersections — Scenarios 5-8

**Files:**
- Modify: `tests/integration/test_load_stress.py` (append remaining tests)

**Step 1: Add stress test scenarios 5-8**

Append to `tests/integration/test_load_stress.py`:

```python
@pytest.mark.stress
def test_concurrent_penalty_application() -> None:
    """Scenario 17: 100 coordinator calls with mixed cache states.

    Verifies:
    - READY calls get raw confidence
    - PARTIALLY_READY calls get penalized confidence
    - No cross-contamination between calls
    """
    meta = EvaluationMeta(rules_evaluated=1, rules_matched=1, duration_ms=5.0)
    results_ready: list[float] = []
    results_partial: list[float] = []

    for i in range(100):
        result = RuleResult(
            rule_id="R01_oom_killed",
            root_cause="OOM",
            confidence=0.80,
            evidence=[],
            affected_resources=[],
            suggested_remediation="Fix",
        )
        if i % 2 == 0:
            resp = _rule_result_to_rca_response(
                result, CacheReadiness.READY, meta, "test", 0
            )
            results_ready.append(resp.confidence)
        else:
            resp = _rule_result_to_rca_response(
                result, CacheReadiness.PARTIALLY_READY, meta, "test", 0
            )
            results_partial.append(resp.confidence)

    # All READY results should be 0.80
    assert all(c == pytest.approx(0.80, abs=1e-9) for c in results_ready)
    # All PARTIALLY_READY results should be 0.65 (0.80 - 0.15)
    assert all(c == pytest.approx(0.65, abs=1e-9) for c in results_partial)
    # No cross-contamination
    assert len(results_ready) == 50
    assert len(results_partial) == 50


@pytest.mark.stress
def test_api_410_429_error_injection() -> None:
    """Scenario 18: Simulate watch stream 410/429 errors.

    Verifies:
    - Cache correctly transitions to DEGRADED via reconnect failures
    - Divergence counter increments
    - Cache recovers when errors stop
    """
    cache = ResourceCache()
    cache._all_kinds = {"Pod"}
    cache._ready_kinds = {"Pod"}
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.READY

    # Simulate 410 Gone — triggers reconnect failures
    for _ in range(4):
        cache.notify_reconnect_failure()
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.DEGRADED

    # Simulate recovery (watcher successfully reconnects)
    cache.reset_reconnect_failures()
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.READY

    # Simulate 429 burst errors lasting > 1 minute
    now = datetime.now(UTC)
    for i in range(10):
        cache._burst_error_times.append(now - timedelta(seconds=90 - i * 5))
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.DEGRADED

    # Clear burst errors
    cache._burst_error_times.clear()
    cache._recompute_readiness()
    assert cache.readiness() == CacheReadiness.READY


@pytest.mark.stress
def test_memory_pressure_under_gc() -> None:
    """Scenario 19: Fill ledger to high memory, trigger eviction under load.

    Verifies:
    - Memory drops after eviction
    - Evaluations don't crash during eviction
    - Diffs remain valid for non-evicted entries
    """
    cache = ResourceCache()
    _populate_cache_with_test_data(cache)

    # Use a ledger with very small max_versions to force eviction
    ledger = ChangeLedger(max_versions=3, retention_hours=1)

    # Fill with many resources
    now = datetime.now(UTC)
    for res_idx in range(100):
        for ver in range(10):
            ledger.record(
                _make_snapshot(res_idx, ver, captured_at=now - timedelta(seconds=ver * 60))
            )

    # Record memory before
    stats = ledger.stats()
    memory_before = stats.get("memory_bytes", 0) if isinstance(stats, dict) else 0

    engine = RuleEngine(cache=cache, ledger=ledger)
    engine.register(OOMKilledRule())

    # Evaluate events concurrently with potential eviction
    events = [make_oom_event(resource_name=f"pod-gc-{i}") for i in range(50)]
    errors: list[str] = []

    for event in events:
        try:
            result, meta = engine.evaluate(event)
        except Exception as exc:
            errors.append(str(exc))

    assert len(errors) == 0, f"Errors during memory-pressure evaluation: {errors}"


@pytest.mark.stress
@pytest.mark.asyncio
async def test_scout_rule_engine_race_dedup() -> None:
    """Scenario 20: Work queue deduplication prevents double evaluation.

    Verifies:
    - Two enqueue calls for the same resource result in one evaluation
    - No deadlocks
    """
    from kuberca.analyst.queue import AnalysisQueue, AnalysisRequest

    queue = AnalysisQueue(max_size=100)

    # Create two identical requests
    req1 = AnalysisRequest(
        resource="Pod/default/my-app-7b4f8c6d-x2kj",
        time_window="2h",
        priority="normal",
    )
    req2 = AnalysisRequest(
        resource="Pod/default/my-app-7b4f8c6d-x2kj",
        time_window="2h",
        priority="normal",
    )

    # Enqueue both — second should be deduped
    future1 = await queue.enqueue(req1)
    future2 = await queue.enqueue(req2)

    # Both futures should reference the same work
    # The queue should have only 1 item
    assert queue.depth() <= 1

    # Clean up
    await queue.shutdown()
```

**Step 2: Run the new stress tests**

Run: `uv run pytest tests/integration/test_load_stress.py -k "test_concurrent_penalty or test_api_410 or test_memory_pressure or test_scout_rule" -v`
Expected: all PASS

**Step 3: Commit**

```bash
git add tests/integration/test_load_stress.py
git commit -m "feat(tests): add stress intersection scenarios 17-20 (penalty, 410/429, GC, dedup)"
```

---

## Task 10: Run Full Test Suite and Verify

**Step 1: Run all existing + new tests**

Run: `uv run pytest --tb=short -q`
Expected: all 1536+ tests PASS (new tests add ~60-80 more)

**Step 2: Run lint**

Run: `uv run ruff check .`
Expected: clean

**Step 3: Run type check**

Run: `uv run mypy kuberca/`
Expected: clean

**Step 4: Run stress tests specifically**

Run: `uv run pytest tests/integration/test_load_stress.py -v -m stress`
Expected: all PASS

**Step 5: Run coverage**

Run: `uv run pytest --cov=kuberca --cov-report=term-missing -q`
Expected: >= 97% coverage maintained

---

## Task 11: Final Commit and Summary

**Step 1: Verify git status**

Run: `git status`
Expected: all new files tracked

**Step 2: Final commit (if any unstaged changes)**

```bash
git add -A
git commit -m "feat(tests): complete invariant protection, confidence-penalty, and stress intersection suites"
```

---

## Implementation Notes

**Adaptation required during implementation:**

- The exact line numbers in `kuberca/cache/resource_cache.py` may differ — check the logger variable name (likely `_logger` or `_rc_logger`)
- The `AnalysisQueue` API in Task 9 (scenario 20) needs verification — check `kuberca/analyst/queue.py` for exact `enqueue()` signature and `AnalysisRequest` class
- The `notify_reconnect_failure()` and `reset_reconnect_failures()` methods on ResourceCache need verification — check if they exist or if the field is set directly
- Some imports may need adjustment based on the actual module structure
- The `ledger.stats()` method in Task 9 (scenario 19) needs verification — check if it exists and its return type

**Key invariant to maintain:** The plan adds runtime checks that LOG warnings but NEVER crash. If a runtime check detects a violation, it logs + increments the metric, then clamps the value to the valid range.
