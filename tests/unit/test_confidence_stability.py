"""Adversarial stability tests for the 4-band confidence strategy.

Proves that the rule engine produces deterministic, stable results when
multiple rules compete over the same event in the 0.70-0.84 confidence band.

Covers four competing pairs:
  1. R01 OOMKilled (pri=10) vs R09 NodePressure (pri=15)  — engine isolation proof
  2. R02 CrashLoop  (pri=10) vs R07 ConfigDrift (pri=20)  — engine isolation proof
  3. R08 VolumeMount (pri=20) vs R11 FailedMountCM (pri=20) — same-priority tie-break
  4. R09 NodePressure (pri=15) vs R17 Evicted (pri=18)    — engine isolation proof

For pairs 1, 2, and 4 the match() methods are mutually exclusive (different
event.reason values), so the stability tests use lightweight stub rules that
can both match the same event — exercising the engine's priority / tie-break
logic without depending on the exact reasons.  Pair 3 is tested end-to-end
because R08 and R11 genuinely share the "FailedMount + configmap" trigger.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from kuberca.models.analysis import AffectedResource, CorrelationResult, EvidenceItem, RuleResult
from kuberca.models.events import EventRecord, EventSource, EvidenceType, Severity
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule, RuleEngine
from kuberca.rules.confidence import compute_confidence
from kuberca.rules.r08_volume_mount import VolumeMountRule
from kuberca.rules.r11_failedmount_configmap import FailedMountConfigMapRule

# ---------------------------------------------------------------------------
# Shared timestamps
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 2, 21, 10, 0, 0, tzinfo=UTC)
_15_MIN_AGO = _NOW - timedelta(minutes=15)
_1H_AGO = _NOW - timedelta(hours=1)


# ---------------------------------------------------------------------------
# Shared factory helpers
# ---------------------------------------------------------------------------


def _make_event(
    reason: str = "FailedMount",
    message: str = "test event",
    namespace: str = "default",
    name: str = "my-app-7b4f8c6d-x2kj",
    count: int = 3,
    last_seen: datetime | None = None,
) -> EventRecord:
    seen = last_seen or _NOW
    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message=message,
        namespace=namespace,
        resource_kind="Pod",
        resource_name=name,
        first_seen=seen - timedelta(hours=1),
        last_seen=seen,
        count=count,
    )


def _make_resource_view(
    kind: str = "Deployment",
    namespace: str = "default",
    name: str = "my-app",
    spec: dict[str, object] | None = None,
    status: dict[str, object] | None = None,
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
        last_updated=_NOW,
    )


def _make_field_change(
    field_path: str = "spec.template.spec.containers[0].resources.limits.memory",
    old_value: str = "128Mi",
    new_value: str = "256Mi",
    changed_at: datetime | None = None,
) -> FieldChange:
    return FieldChange(
        field_path=field_path,
        old_value=old_value,
        new_value=new_value,
        changed_at=changed_at or _15_MIN_AGO,
    )


def _stub_cache(
    get_return: CachedResourceView | None = None,
    list_return: list[CachedResourceView] | None = None,
) -> MagicMock:
    cache = MagicMock()
    cache.get.return_value = get_return
    cache.list.return_value = list_return or []
    cache.list_by_label.return_value = []
    cache.list_truncated.return_value = False
    return cache


def _stub_ledger(diff_return: list[FieldChange] | None = None) -> MagicMock:
    ledger = MagicMock()
    ledger.diff.return_value = diff_return or []
    ledger.latest.return_value = None
    return ledger


# ---------------------------------------------------------------------------
# Stub rules for determinism testing
# Lightweight rules that always match any event so we can force two rules to
# compete in the medium-confidence band.
# ---------------------------------------------------------------------------


class _StubRule(Rule):
    """Generic stub rule: always matches, returns a configurable confidence.

    correlate() always returns 2 related resources so the locality bonus (+0.05)
    is suppressed.  This keeps scores predictable: they depend only on
    base_confidence, presence of changes, timing, and recurrence count.
    """

    rule_id: str = "STUB_BASE"
    display_name: str = "Stub Base"
    priority: int = 10
    base_confidence: float = 0.55
    resource_dependencies: list[str] = ["Pod"]
    relevant_field_paths: list[str] = ["spec"]

    def __init__(
        self,
        rule_id: str,
        priority: int,
        base_confidence: float,
        resource_dependencies: list[str] | None = None,
    ) -> None:
        # Override class attributes per instance so each stub is independent.
        self.rule_id = rule_id
        self.priority = priority
        self.base_confidence = base_confidence
        self.resource_dependencies = resource_dependencies or ["Pod"]

    def match(self, event: EventRecord) -> bool:
        return True

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        changes = ledger.diff("Pod", event.namespace, event.resource_name, since_hours=2.0)
        # Return 2 related resources so is_single_resource() returns False
        # and the locality bonus (+0.05) is suppressed — keeps scores deterministic.
        return CorrelationResult(
            changes=list(changes),
            related_events=[],
            related_resources=[
                _make_resource_view("Pod", event.namespace, event.resource_name),
                _make_resource_view("Deployment", event.namespace, "app-deploy"),
            ],
            objects_queried=2,
            duration_ms=0.1,
        )

    def explain(self, event: EventRecord, correlation: CorrelationResult) -> RuleResult:
        confidence = compute_confidence(self, correlation, event)
        return RuleResult(
            rule_id=self.rule_id,
            root_cause=f"Stub diagnosis from {self.rule_id}",
            confidence=confidence,
            evidence=[
                EvidenceItem(
                    type=EvidenceType.EVENT,
                    timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    summary=f"Event: {event.reason}",
                )
            ],
            affected_resources=[AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)],
            suggested_remediation="No remediation available.",
        )


# ---------------------------------------------------------------------------
# Test class: compute_confidence idempotency
# ---------------------------------------------------------------------------


class TestComputeConfidenceIdempotency:
    """compute_confidence must produce identical output for identical inputs."""

    def test_1000_calls_identical_output_all_bonuses(self) -> None:
        """1000 repeated calls with all bonuses active produce the same score."""
        change = _make_field_change(changed_at=_15_MIN_AGO)
        resource = _make_resource_view()
        corr = CorrelationResult(
            changes=[change],
            related_events=[],
            related_resources=[resource],
            objects_queried=1,
            duration_ms=0.5,
        )
        event = _make_event(count=5, last_seen=_NOW)
        rule = _StubRule("IDEMPOTENT_R", 10, 0.55)

        first_result = compute_confidence(rule, corr, event)
        for _ in range(999):
            result = compute_confidence(rule, corr, event)
            assert result == first_result, f"compute_confidence returned {result} but expected {first_result}"

    def test_1000_calls_identical_output_no_bonuses(self) -> None:
        """1000 calls with bare base_confidence and no bonuses produce the same score."""
        corr = CorrelationResult(
            changes=[],
            related_events=[],
            related_resources=[_make_resource_view("a"), _make_resource_view("b")],
            objects_queried=0,
            duration_ms=0.0,
        )
        event = _make_event(count=1, last_seen=_NOW)
        rule = _StubRule("BARE_R", 10, 0.50)

        first_result = compute_confidence(rule, corr, event)
        for _ in range(999):
            result = compute_confidence(rule, corr, event)
            assert result == first_result

    def test_bare_base_confidence_equals_base_when_two_resources(self) -> None:
        """No-bonus path: 2 resources, count=1, no changes → score == base_confidence."""
        corr = CorrelationResult(
            changes=[],
            related_events=[],
            related_resources=[_make_resource_view("a"), _make_resource_view("b")],
            objects_queried=0,
            duration_ms=0.0,
        )
        event = _make_event(count=1)
        rule = _StubRule("BARE_EXACT", 10, 0.55)
        assert compute_confidence(rule, corr, event) == pytest.approx(0.55, abs=1e-9)

    def test_all_bonuses_hit_cap(self) -> None:
        """base=0.60 + all bonuses (0.35) = 0.95 (capped)."""
        change = _make_field_change(changed_at=_15_MIN_AGO)
        corr = CorrelationResult(
            changes=[change],
            related_events=[],
            related_resources=[_make_resource_view()],
            objects_queried=1,
            duration_ms=0.0,
        )
        event = _make_event(count=5, last_seen=_NOW)
        rule = _StubRule("CAP_TEST", 10, 0.60)
        score = compute_confidence(rule, corr, event)
        assert score == pytest.approx(0.95, abs=1e-9)

    def test_high_base_alone_capped(self) -> None:
        """base=0.95 with no bonuses except locality still capped at 0.95."""
        corr = CorrelationResult(
            changes=[],
            related_events=[],
            related_resources=[],
            objects_queried=0,
            duration_ms=0.0,
        )
        event = _make_event(count=1)
        rule = _StubRule("HIGH_BASE", 10, 0.95)
        assert compute_confidence(rule, corr, event) == pytest.approx(0.95, abs=1e-9)


# ---------------------------------------------------------------------------
# Test class: engine priority ordering
# ---------------------------------------------------------------------------


class TestEngineRegistrationOrder:
    """Verify (priority, rule_id) sort key is enforced regardless of register order."""

    def test_lower_priority_evaluated_first_regardless_of_registration_order(self) -> None:
        """Registering high-priority rule first doesn't change evaluation order."""
        cache = _stub_cache()
        ledger = _stub_ledger()
        engine = RuleEngine(cache=cache, ledger=ledger)

        # Register in reverse priority order
        high_priority = _StubRule("R_HIGH", priority=10, base_confidence=0.55)
        low_priority = _StubRule("R_LOW", priority=20, base_confidence=0.55)
        engine.register(low_priority)
        engine.register(high_priority)

        # Internal list must be sorted: R_HIGH (10) before R_LOW (20)
        ids = [r.rule_id for r in engine._rules]
        assert ids.index("R_HIGH") < ids.index("R_LOW")

    def test_equal_priority_sorted_by_rule_id_lexicographically(self) -> None:
        """Tie-break: same priority → alphabetical rule_id order."""
        cache = _stub_cache()
        ledger = _stub_ledger()
        engine = RuleEngine(cache=cache, ledger=ledger)

        rule_z = _StubRule("Z_RULE", priority=20, base_confidence=0.55)
        rule_a = _StubRule("A_RULE", priority=20, base_confidence=0.55)
        rule_m = _StubRule("M_RULE", priority=20, base_confidence=0.55)
        engine.register(rule_z)
        engine.register(rule_m)
        engine.register(rule_a)

        ids = [r.rule_id for r in engine._rules]
        assert ids == ["A_RULE", "M_RULE", "Z_RULE"]

    def test_r08_before_r11_in_same_priority_sort(self) -> None:
        """R08_volume_mount < R11_failedmount_configmap alphabetically at priority 20."""
        assert "R08_volume_mount" < "R11_failedmount_configmap"
        cache = _stub_cache()
        ledger = _stub_ledger()
        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(FailedMountConfigMapRule())
        engine.register(VolumeMountRule())
        ids = [r.rule_id for r in engine._rules]
        assert ids.index("R08_volume_mount") < ids.index("R11_failedmount_configmap")


# ---------------------------------------------------------------------------
# Test class: Pair 1 — R01 vs R09 stub competition
# ---------------------------------------------------------------------------


class TestPair1OOMvsNodePressureStubs:
    """Prove R01-equivalent wins over R09-equivalent via priority ordering.

    Both stub rules always match; R01 (pri=10) must always beat R09 (pri=15)
    when both land in the medium-confidence (0.70-0.84) band.
    """

    def _build_engine_with_medium_confidence_change(
        self,
    ) -> tuple[RuleEngine, EventRecord]:
        """Return an engine and event configured so both stubs land at ~0.70."""
        # A change outside the 30-min window gives +0.15 diff but NOT +0.10 timing.
        # base=0.55 + diff=0.15 + recurrence=0.05 (count=3) = 0.75 → medium band.
        change = _make_field_change(
            changed_at=_NOW - timedelta(hours=2),
        )
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        r01_stub = _StubRule(
            "R01_oom_killed",
            priority=10,
            base_confidence=0.55,
            resource_dependencies=["Pod", "Deployment", "ReplicaSet"],
        )
        r09_stub = _StubRule(
            "R09_node_pressure", priority=15, base_confidence=0.55, resource_dependencies=["Node", "Pod"]
        )

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(r09_stub)
        engine.register(r01_stub)

        event = _make_event(reason="OOMKilled", count=3, last_seen=_NOW)
        return engine, event

    def test_r01_wins_over_r09_stub_in_medium_band(self) -> None:
        """Engine selects R01 (priority=10) over R09 (priority=15) stub."""
        engine, event = self._build_engine_with_medium_confidence_change()
        result, meta = engine.evaluate(event)

        assert result is not None
        assert result.rule_id == "R01_oom_killed"
        assert 0.70 <= result.confidence < 0.85

    def test_r01_vs_r09_winner_is_stable_over_100_replays(self) -> None:
        """Identical inputs always yield the same rule_id and confidence."""
        engine, event = self._build_engine_with_medium_confidence_change()

        first_result, _ = engine.evaluate(event)
        assert first_result is not None
        first_rule_id = first_result.rule_id
        first_confidence = first_result.confidence

        for i in range(99):
            result, _ = engine.evaluate(event)
            assert result is not None, f"Replay {i + 1}: engine returned None unexpectedly"
            assert result.rule_id == first_rule_id, (
                f"Replay {i + 1}: got rule_id={result.rule_id!r}, expected {first_rule_id!r}"
            )
            assert result.confidence == pytest.approx(first_confidence, abs=1e-9), (
                f"Replay {i + 1}: got confidence={result.confidence}, expected {first_confidence}"
            )

    def test_medium_band_activates_competing_deps_mode(self) -> None:
        """After R01-stub (medium confidence), engine restricts to Pod-sharing rules."""
        change = _make_field_change(changed_at=_NOW - timedelta(hours=2))
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        # R01-stub shares Pod with R09-stub → R09 still evaluated.
        # An unrelated stub (no Pod dep) must be SKIPPED.
        r01_stub = _StubRule(
            "R01_oom_killed", priority=10, base_confidence=0.55, resource_dependencies=["Pod", "Deployment"]
        )
        r09_stub = _StubRule(
            "R09_node_pressure", priority=15, base_confidence=0.55, resource_dependencies=["Node", "Pod"]
        )
        unrelated_stub = _StubRule("UNRELATED", priority=12, base_confidence=0.55, resource_dependencies=["Service"])

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(r01_stub)
        engine.register(unrelated_stub)
        engine.register(r09_stub)

        event = _make_event(count=3)
        result, meta = engine.evaluate(event)

        # UNRELATED shares no dep with R01 → it should be skipped.
        # rules_matched should be at most 2 (R01 + R09), not 3.
        assert meta.rules_matched <= 2


# ---------------------------------------------------------------------------
# Test class: Pair 2 — R02 vs R07 stub competition
# ---------------------------------------------------------------------------


class TestPair2CrashLoopVsConfigDriftStubs:
    """Engine behavior when R02-equivalent (pri=10, base=0.50) competes with R07-equivalent (pri=20, base=0.55).

    Scenario: R02 fires first (lower priority value) and scores 0.75 (medium band).
    Competing-deps mode activates; R07 shares ConfigMap+Pod so it is still evaluated.
    R07 scores 0.80 — higher than R02 — so the engine picks R07 as the best candidate.
    The winner is DETERMINISTIC and stable across replays.
    """

    def _build_engine(self) -> tuple[RuleEngine, EventRecord]:
        # Change outside 30-min window: +diff(0.15), no timing bonus.
        # R02: 0.50 + 0.15 + recurrence(count=3,0.05) = 0.70 → medium.
        # R07: 0.55 + 0.15 + recurrence(count=3,0.05) = 0.75 → medium.
        # Both in medium band; R07 has higher confidence → R07 wins.
        change = _make_field_change(
            field_path="metadata.data.DB_HOST",
            changed_at=_NOW - timedelta(hours=2),
        )
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        r02_stub = _StubRule(
            "R02_crash_loop",
            priority=10,
            base_confidence=0.50,
            resource_dependencies=["Pod", "Deployment", "ReplicaSet", "ConfigMap"],
        )
        r07_stub = _StubRule(
            "R07_config_drift",
            priority=20,
            base_confidence=0.55,
            resource_dependencies=["Pod", "Deployment", "ConfigMap"],
        )
        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(r07_stub)
        engine.register(r02_stub)

        event = _make_event(
            reason="BackOff",
            message="Back-off restarting failed container after configmap change",
            count=3,
        )
        return engine, event

    def test_r07_stub_wins_due_to_higher_confidence(self) -> None:
        """R07 (base=0.55) outscores R02 (base=0.50) in the medium band — engine picks highest confidence."""
        engine, event = self._build_engine()
        result, meta = engine.evaluate(event)

        assert result is not None
        # R02 scores 0.70, R07 scores 0.75 — engine returns the higher one.
        assert result.rule_id == "R07_config_drift"
        assert result.confidence > 0.70

    def test_r02_vs_r07_stable_over_100_replays(self) -> None:
        """100 identical replays all return the same rule_id and confidence."""
        engine, event = self._build_engine()
        first, _ = engine.evaluate(event)
        assert first is not None

        for i in range(99):
            result, _ = engine.evaluate(event)
            assert result is not None
            assert result.rule_id == first.rule_id, f"Replay {i + 1} diverged"
            assert result.confidence == pytest.approx(first.confidence, abs=1e-9)

    def test_r02_stub_still_evaluated_before_r07_stub(self) -> None:
        """R02 (priority=10) is always evaluated before R07 (priority=20)."""
        cache = _stub_cache()
        ledger = _stub_ledger()
        engine = RuleEngine(cache=cache, ledger=ledger)
        r02_stub = _StubRule(
            "R02_crash_loop", priority=10, base_confidence=0.50, resource_dependencies=["Pod", "ConfigMap"]
        )
        r07_stub = _StubRule(
            "R07_config_drift", priority=20, base_confidence=0.55, resource_dependencies=["Pod", "ConfigMap"]
        )
        engine.register(r07_stub)
        engine.register(r02_stub)
        ids = [r.rule_id for r in engine._rules]
        assert ids.index("R02_crash_loop") < ids.index("R07_config_drift")


# ---------------------------------------------------------------------------
# Test class: Pair 3 — R08 VolumeMount vs R11 FailedMountCM (real rules)
# ---------------------------------------------------------------------------


class TestPair3R08vsR11RealRules:
    """Both R08 and R11 can genuinely co-match a FailedMount+configmap event.

    Same priority (20); R08_volume_mount < R11_failedmount_configmap lexically,
    so R08 must win when confidence ties.
    """

    def _build_engine(self) -> tuple[RuleEngine, EventRecord]:
        """Wire R08 and R11 with a FailedMount-configmap event and no PVC noise."""
        cache = MagicMock()
        # R11 tries cache.get("ConfigMap", ...) — return None (not found)
        # R08 tries cache.get("PersistentVolumeClaim", ...) — return None
        # R08 also calls cache.list("PersistentVolumeClaim", ...) — empty list
        cache.get.return_value = None
        cache.list.return_value = []
        cache.list_by_label.return_value = []
        cache.list_truncated.return_value = False

        ledger = _stub_ledger()

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(FailedMountConfigMapRule())  # R11
        engine.register(VolumeMountRule())  # R08

        # Event that triggers both: FailedMount + "configmap" in message.
        # count=3 → +0.05 recurrence; no changes, no single resource.
        event = _make_event(
            reason="FailedMount",
            message="Unable to mount volumes for pod: configmap my-config not found",
            count=3,
        )
        return engine, event

    def test_r08_wins_over_r11_by_rule_id_tiebreak(self) -> None:
        """R08 and R11 share priority=20; R08 wins via lexicographic rule_id sort."""
        engine, event = self._build_engine()
        result, meta = engine.evaluate(event)

        assert result is not None
        assert result.rule_id == "R08_volume_mount", f"Expected R08 to win tie-break, got {result.rule_id!r}"

    def test_r08_vs_r11_stable_over_100_replays(self) -> None:
        """100 replays of the same FailedMount-configmap event produce identical results."""
        engine, event = self._build_engine()
        first, _ = engine.evaluate(event)
        assert first is not None

        for i in range(99):
            result, _ = engine.evaluate(event)
            assert result is not None
            assert result.rule_id == first.rule_id, f"Replay {i + 1} diverged"
            assert result.confidence == pytest.approx(first.confidence, abs=1e-9)

    def test_r08_confidence_deterministic_for_failedmount_configmap(self) -> None:
        """R08 explain() with no changes and count=3 produces a specific reproducible score.

        base=0.60, no diff, no 30-min change, recurrence (count>=3) +0.05,
        no related resources (0 PVCs unbound, 0 from cache.list) → locality +0.05
        = 0.70 exactly.
        """
        engine, event = self._build_engine()
        result, _ = engine.evaluate(event)

        assert result is not None
        assert result.confidence == pytest.approx(0.70, abs=1e-9)

    def test_both_rules_actually_match_failedmount_configmap_event(self) -> None:
        """Verify the event triggers both R08 and R11 match() methods."""
        event = _make_event(
            reason="FailedMount",
            message="Unable to mount volumes for pod: configmap my-config not found",
            count=3,
        )
        assert VolumeMountRule().match(event) is True
        assert FailedMountConfigMapRule().match(event) is True


# ---------------------------------------------------------------------------
# Test class: Pair 4 — R09 NodePressure vs R17 Evicted stub competition
# ---------------------------------------------------------------------------


class TestPair4NodePressureVsEvictedStubs:
    """Engine behavior when R09-equivalent (pri=15, base=0.55) competes with R17-equivalent (pri=18, base=0.60).

    Scenario: R09 fires first (priority=15) and scores 0.80 (medium band, diff within 30-min window).
    Competing-deps mode activates; R17 shares Node+Pod so it is evaluated.
    R17 scores 0.85 → short-circuit band → engine returns R17 immediately.
    The winner is DETERMINISTIC across replays.

    For the alternative scenario where both land in medium band (no timing bonus),
    R09 scores 0.75 and R17 scores 0.80 — R17 still wins via higher confidence.
    """

    def _build_engine_r17_wins_via_short_circuit(self) -> tuple[RuleEngine, EventRecord]:
        """R09 medium → R17 triggers short-circuit at ≥0.85."""
        # Change within 30-min window → timing bonus applies.
        # R09: 0.55 + diff(0.15) + timing(0.10) = 0.80 (medium, <0.85).
        # R17: 0.60 + diff(0.15) + timing(0.10) = 0.85 → short-circuit.
        change = _make_field_change(
            field_path="status.conditions",
            changed_at=_15_MIN_AGO,
        )
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        r09_stub = _StubRule(
            "R09_node_pressure",
            priority=15,
            base_confidence=0.55,
            resource_dependencies=["Node", "Pod"],
        )
        r17_stub = _StubRule(
            "R17_evicted",
            priority=18,
            base_confidence=0.60,
            resource_dependencies=["Pod", "Node"],
        )
        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(r17_stub)
        engine.register(r09_stub)

        event = _make_event(reason="Evicted", count=1, last_seen=_NOW)
        return engine, event

    def _build_engine_both_medium_band(self) -> tuple[RuleEngine, EventRecord]:
        """Both rules land in medium band (0.70-0.84). R17 wins via higher confidence."""
        # Change outside 30-min window → no timing bonus.
        # R09: 0.55 + diff(0.15) + recurrence(count=3,0.05) = 0.75 (medium).
        # R17: 0.60 + diff(0.15) + recurrence(count=3,0.05) = 0.80 (medium).
        # Both in medium band; R17 higher → R17 wins.
        change = _make_field_change(
            field_path="status.conditions",
            changed_at=_NOW - timedelta(hours=2),
        )
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        r09_stub = _StubRule(
            "R09_node_pressure",
            priority=15,
            base_confidence=0.55,
            resource_dependencies=["Node", "Pod"],
        )
        r17_stub = _StubRule(
            "R17_evicted",
            priority=18,
            base_confidence=0.60,
            resource_dependencies=["Pod", "Node"],
        )
        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(r17_stub)
        engine.register(r09_stub)

        event = _make_event(reason="Evicted", count=3, last_seen=_NOW)
        return engine, event

    def test_r17_wins_via_short_circuit_when_confidence_hits_085(self) -> None:
        """R17 (base=0.60) reaches ≥0.85 → short-circuit; engine returns R17."""
        engine, event = self._build_engine_r17_wins_via_short_circuit()
        result, meta = engine.evaluate(event)

        assert result is not None
        assert result.rule_id == "R17_evicted"
        assert result.confidence >= 0.85

    def test_r17_wins_in_medium_band_via_higher_confidence(self) -> None:
        """R17 (0.80) beats R09 (0.75) when both are in the medium band."""
        engine, event = self._build_engine_both_medium_band()
        result, _ = engine.evaluate(event)

        assert result is not None
        assert result.rule_id == "R17_evicted"
        assert 0.70 <= result.confidence < 0.85

    def test_r09_vs_r17_stable_over_100_replays(self) -> None:
        """100 replays with the medium-band scenario always return the same result."""
        engine, event = self._build_engine_both_medium_band()
        first, _ = engine.evaluate(event)
        assert first is not None

        for i in range(99):
            result, _ = engine.evaluate(event)
            assert result is not None
            assert result.rule_id == first.rule_id, f"Replay {i + 1} diverged"
            assert result.confidence == pytest.approx(first.confidence, abs=1e-9)

    def test_r09_evaluated_before_r17_by_priority(self) -> None:
        """R09 (priority=15) is always evaluated before R17 (priority=18) in sort order."""
        cache = _stub_cache()
        ledger = _stub_ledger()
        engine = RuleEngine(cache=cache, ledger=ledger)
        r09 = _StubRule("R09_node_pressure", priority=15, base_confidence=0.55, resource_dependencies=["Node", "Pod"])
        r17 = _StubRule("R17_evicted", priority=18, base_confidence=0.60, resource_dependencies=["Pod", "Node"])
        engine.register(r17)
        engine.register(r09)
        ids = [r.rule_id for r in engine._rules]
        assert ids.index("R09_node_pressure") < ids.index("R17_evicted")


# ---------------------------------------------------------------------------
# Test class: engine band transitions
# ---------------------------------------------------------------------------


class TestEngineBandTransitions:
    """Verify the 4-band strategy transitions correctly under each confidence level."""

    def test_short_circuit_on_high_confidence(self) -> None:
        """A result >= 0.85 causes immediate return without evaluating further rules."""
        # base=0.55 + diff(0.15) + timing(0.10) + recurrence(0.05) + locality(0.05) = 0.90
        change = _make_field_change(changed_at=_15_MIN_AGO)
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        first_rule = _StubRule("FIRST", priority=10, base_confidence=0.55)
        second_rule = _StubRule("SECOND", priority=20, base_confidence=0.55)

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(first_rule)
        engine.register(second_rule)

        event = _make_event(count=5, last_seen=_NOW)
        result, meta = engine.evaluate(event)

        assert result is not None
        assert result.confidence >= 0.85
        # Short-circuit: only the first rule should have been matched.
        assert meta.rules_matched == 1

    def test_low_confidence_continues_all_rules(self) -> None:
        """A result < 0.70 does NOT restrict subsequent rule evaluation."""
        # base=0.50, no bonuses (count=1, 2 resources, no changes) = 0.50 → low band.
        cache = _stub_cache()
        ledger = _stub_ledger()

        low_conf_rule = _StubRule("LOW_CONF", priority=10, base_confidence=0.50, resource_dependencies=["Pod"])
        # Completely disjoint deps — would be skipped in competing-only mode.
        unrelated_rule = _StubRule(
            "UNRELATED_DEPS", priority=20, base_confidence=0.60, resource_dependencies=["Service"]
        )

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(low_conf_rule)
        engine.register(unrelated_rule)

        # count=1, 2 related resources → no bonuses → 0.50 (low band)
        event = _make_event(count=1)

        # Override correlate for LOW_CONF to return 2 resources so no locality bonus.
        def _low_corr(event: EventRecord, cache: object, ledger: object) -> CorrelationResult:
            return CorrelationResult(
                changes=[],
                related_events=[],
                related_resources=[_make_resource_view("a"), _make_resource_view("b")],
                objects_queried=2,
                duration_ms=0.1,
            )

        low_conf_rule.correlate = _low_corr  # type: ignore[method-assign]

        result, meta = engine.evaluate(event)

        # Both rules must be evaluated: low confidence doesn't restrict.
        assert meta.rules_matched == 2

    def test_medium_confidence_restricts_to_shared_deps(self) -> None:
        """First medium-confidence match causes unrelated rules to be skipped."""
        change = _make_field_change(changed_at=_NOW - timedelta(hours=2))
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        medium_rule = _StubRule(
            "MEDIUM", priority=10, base_confidence=0.55, resource_dependencies=["Pod", "Deployment"]
        )
        shared_dep_rule = _StubRule("SHARED", priority=20, base_confidence=0.55, resource_dependencies=["Pod"])
        disjoint_rule = _StubRule("DISJOINT", priority=15, base_confidence=0.55, resource_dependencies=["Service"])

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(medium_rule)
        engine.register(disjoint_rule)
        engine.register(shared_dep_rule)

        event = _make_event(count=3)
        _, meta = engine.evaluate(event)

        # DISJOINT shares no dep with MEDIUM → must be skipped.
        # SHARED shares "Pod" → must be evaluated.
        # rules_matched should be exactly 2 (MEDIUM + SHARED), not 3.
        assert meta.rules_matched == 2

    def test_confidence_cap_never_triggers_short_circuit_falsely(self) -> None:
        """A score >= 0.85 (including capped scores) always triggers short-circuit.

        The engine must return the first result that hits the short-circuit band,
        even when the score is capped at 0.95 by the formula.  The second rule
        must never be evaluated.
        """
        change = _make_field_change(changed_at=_15_MIN_AGO)
        cache = _stub_cache()
        ledger = _stub_ledger(diff_return=[change])

        # Stub returns 2 related resources (locality suppressed).
        # base=0.60 + diff(0.15) + timing(0.10) + recurrence(count=5,0.05) = 0.90 → short-circuit.
        capping_rule = _StubRule("CAP_RULE", priority=10, base_confidence=0.60)
        second_rule = _StubRule("AFTER_CAP", priority=20, base_confidence=0.55)

        engine = RuleEngine(cache=cache, ledger=ledger)
        engine.register(capping_rule)
        engine.register(second_rule)

        event = _make_event(count=5, last_seen=_NOW)
        result, meta = engine.evaluate(event)

        assert result is not None
        assert result.rule_id == "CAP_RULE"
        assert result.confidence >= 0.85  # short-circuit threshold
        assert meta.rules_matched == 1  # second rule skipped


# ---------------------------------------------------------------------------
# Test class: R08 vs R11 confidence formula verification (real rules, no engine)
# ---------------------------------------------------------------------------


class TestPair3ConfidenceFormula:
    """Verify the exact confidence values R08 and R11 produce in isolation."""

    def test_r08_base_plus_recurrence_plus_locality(self) -> None:
        """R08: base=0.60, no diff, no 30-min, count=3, 0 related → 0.70."""
        rule = VolumeMountRule()
        event = _make_event(reason="FailedMount", count=3)
        corr = CorrelationResult(
            changes=[],
            related_events=[],
            related_resources=[],
            objects_queried=0,
            duration_ms=0.0,
        )
        score = compute_confidence(rule, corr, event)
        # 0.60 + recurrence=0.05 + locality=0.05 = 0.70
        assert score == pytest.approx(0.70, abs=1e-9)

    def test_r11_base_plus_recurrence_plus_locality(self) -> None:
        """R11: base=0.60, no diff, no 30-min, count=3, 0 related → 0.70."""
        rule = FailedMountConfigMapRule()
        event = _make_event(reason="FailedMount", count=3)
        corr = CorrelationResult(
            changes=[],
            related_events=[],
            related_resources=[],
            objects_queried=0,
            duration_ms=0.0,
        )
        score = compute_confidence(rule, corr, event)
        # same formula → same score
        assert score == pytest.approx(0.70, abs=1e-9)

    def test_r08_confidence_stable_across_1000_calls(self) -> None:
        """compute_confidence on R08 is truly idempotent."""
        rule = VolumeMountRule()
        event = _make_event(reason="FailedMount", count=3)
        corr = CorrelationResult(
            changes=[],
            related_events=[],
            related_resources=[],
            objects_queried=0,
            duration_ms=0.0,
        )
        expected = compute_confidence(rule, corr, event)
        for _ in range(999):
            assert compute_confidence(rule, corr, event) == expected
