"""Tests for kuberca.rules.confidence.

Covers the deterministic confidence scoring formula defined in
Technical Design Spec Section 4.3:

    score = base_confidence
    + 0.15  if rule-relevant diffs were found
    + 0.10  if any change within 30 min of the incident
    + 0.05  if event.count >= 3 (repeated pattern)
    + 0.05  if at most one related resource (localized)
    → capped at 0.95
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from kuberca.models.analysis import CorrelationResult
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.rules.confidence import (
    any_within_30min,
    compute_confidence,
    is_single_resource,
)

# ---------------------------------------------------------------------------
# Helpers / factories
# ---------------------------------------------------------------------------


def _make_event(
    reason: str = "OOMKilled",
    count: int = 1,
    last_seen: datetime | None = None,
) -> EventRecord:
    if last_seen is None:
        last_seen = datetime.utcnow()
    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=Severity.ERROR,
        reason=reason,
        message="test event",
        namespace="default",
        resource_kind="Pod",
        resource_name="my-pod",
        first_seen=last_seen - timedelta(minutes=1),
        last_seen=last_seen,
        count=count,
    )


def _make_field_change(changed_at: datetime | None = None) -> FieldChange:
    if changed_at is None:
        changed_at = datetime.utcnow()
    return FieldChange(
        field_path="spec.replicas",
        old_value="1",
        new_value="3",
        changed_at=changed_at,
    )


def _make_resource_view(name: str = "my-deploy") -> CachedResourceView:
    return CachedResourceView(
        kind="Deployment",
        namespace="default",
        name=name,
        resource_version="1",
        labels={},
        annotations={},
        spec={},
        status={},
        last_updated=datetime.utcnow(),
    )


def _corr(
    changes: list[FieldChange] | None = None,
    related_resources: list[CachedResourceView] | None = None,
) -> CorrelationResult:
    return CorrelationResult(
        changes=changes or [],
        related_events=[],
        related_resources=related_resources or [],
        objects_queried=0,
        duration_ms=0.0,
    )


class _MockRule:
    """Minimal stand-in for a Rule so we can test compute_confidence."""

    def __init__(self, base_confidence: float = 0.55) -> None:
        self.base_confidence = base_confidence
        self.rule_id = "TEST_RULE"


# ---------------------------------------------------------------------------
# any_within_30min
# ---------------------------------------------------------------------------


class TestAnyWithin30Min:
    def test_change_exactly_30_min_before_event_is_within_window(self) -> None:
        now = datetime.utcnow()
        event = _make_event(last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=30))
        corr = _corr(changes=[change])
        assert any_within_30min(corr, event) is True

    def test_change_31_min_before_event_is_outside_window(self) -> None:
        now = datetime.utcnow()
        event = _make_event(last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=31))
        corr = _corr(changes=[change])
        assert any_within_30min(corr, event) is False

    def test_change_within_30_min_after_event_counts(self) -> None:
        # Clock skew: change logged slightly after event
        now = datetime.utcnow()
        event = _make_event(last_seen=now)
        change = _make_field_change(changed_at=now + timedelta(minutes=10))
        corr = _corr(changes=[change])
        assert any_within_30min(corr, event) is True

    def test_empty_changes_returns_false(self) -> None:
        event = _make_event()
        corr = _corr(changes=[])
        assert any_within_30min(corr, event) is False

    def test_multiple_changes_returns_true_if_any_within_window(self) -> None:
        now = datetime.utcnow()
        event = _make_event(last_seen=now)
        # One old change, one recent change
        changes = [
            _make_field_change(changed_at=now - timedelta(hours=5)),
            _make_field_change(changed_at=now - timedelta(minutes=5)),
        ]
        corr = _corr(changes=changes)
        assert any_within_30min(corr, event) is True


# ---------------------------------------------------------------------------
# is_single_resource
# ---------------------------------------------------------------------------


class TestIsSingleResource:
    def test_zero_resources_is_single(self) -> None:
        corr = _corr(related_resources=[])
        assert is_single_resource(corr) is True

    def test_one_resource_is_single(self) -> None:
        corr = _corr(related_resources=[_make_resource_view()])
        assert is_single_resource(corr) is True

    def test_two_resources_is_not_single(self) -> None:
        corr = _corr(related_resources=[_make_resource_view("a"), _make_resource_view("b")])
        assert is_single_resource(corr) is False

    def test_three_resources_is_not_single(self) -> None:
        corr = _corr(
            related_resources=[
                _make_resource_view("a"),
                _make_resource_view("b"),
                _make_resource_view("c"),
            ]
        )
        assert is_single_resource(corr) is False


# ---------------------------------------------------------------------------
# compute_confidence — individual bonus scenarios
# ---------------------------------------------------------------------------


class TestComputeConfidenceBaseOnly:
    def test_no_bonuses_returns_base_plus_locality(self) -> None:
        # No changes, count=1, 0 related resources (locality bonus applies)
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[])
        score = compute_confidence(rule, corr, event)
        # base=0.55 + locality(0 resources)=0.05 = 0.60
        assert score == pytest.approx(0.60, abs=1e-9)

    def test_base_confidence_respected(self) -> None:
        rule = _MockRule(base_confidence=0.50)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[_make_resource_view(), _make_resource_view("b")])
        score = compute_confidence(rule, corr, event)
        # base=0.50, no bonuses (count=1, no changes, 2 resources)
        assert score == pytest.approx(0.50, abs=1e-9)


class TestComputeConfidenceDiffBonus:
    def test_diff_bonus_added_when_changes_present(self) -> None:
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1, last_seen=datetime.utcnow() - timedelta(hours=1))
        # Change outside 30-min window
        change = _make_field_change(changed_at=datetime.utcnow() - timedelta(hours=2))
        corr = _corr(
            changes=[change],
            related_resources=[_make_resource_view(), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55 + diff=0.15 = 0.70
        assert score == pytest.approx(0.70, abs=1e-9)


class TestComputeConfidenceTimingBonus:
    def test_timing_bonus_added_when_change_within_30_min(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1, last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=10))
        corr = _corr(
            changes=[change],
            related_resources=[_make_resource_view(), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55 + diff=0.15 + timing=0.10 = 0.80
        assert score == pytest.approx(0.80, abs=1e-9)


class TestComputeConfidenceRecurrenceBonus:
    def test_recurrence_bonus_at_count_3(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=3, last_seen=now)
        # Change outside 30-min, no locality bonus
        change = _make_field_change(changed_at=now - timedelta(hours=2))
        corr = _corr(
            changes=[change],
            related_resources=[_make_resource_view(), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55 + diff=0.15 + recurrence=0.05 = 0.75
        assert score == pytest.approx(0.75, abs=1e-9)

    def test_no_recurrence_bonus_at_count_2(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=2, last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(hours=2))
        corr = _corr(
            changes=[change],
            related_resources=[_make_resource_view(), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55 + diff=0.15 = 0.70 (no recurrence because count<3)
        assert score == pytest.approx(0.70, abs=1e-9)


class TestComputeConfidenceLocalityBonus:
    def test_locality_bonus_with_zero_resources(self) -> None:
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[])
        score = compute_confidence(rule, corr, event)
        # base=0.55 + locality=0.05 = 0.60
        assert score == pytest.approx(0.60, abs=1e-9)

    def test_locality_bonus_with_one_resource(self) -> None:
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[_make_resource_view()])
        score = compute_confidence(rule, corr, event)
        # base=0.55 + locality=0.05 = 0.60
        assert score == pytest.approx(0.60, abs=1e-9)

    def test_no_locality_bonus_with_two_resources(self) -> None:
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1)
        corr = _corr(
            changes=[],
            related_resources=[_make_resource_view("a"), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55, no other bonuses = 0.55
        assert score == pytest.approx(0.55, abs=1e-9)


class TestComputeConfidenceCap:
    def test_all_bonuses_capped_at_095(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.60)
        event = _make_event(count=5, last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=5))
        corr = _corr(changes=[change], related_resources=[_make_resource_view()])
        score = compute_confidence(rule, corr, event)
        # base=0.60 + diff=0.15 + timing=0.10 + recurrence=0.05 + locality=0.05 = 0.95
        assert score == pytest.approx(0.95, abs=1e-9)

    def test_score_never_exceeds_095(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.90)
        event = _make_event(count=10, last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=1))
        corr = _corr(changes=[change], related_resources=[])
        score = compute_confidence(rule, corr, event)
        assert score <= 0.95

    def test_high_base_confidence_still_capped(self) -> None:
        rule = _MockRule(base_confidence=0.95)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[])
        score = compute_confidence(rule, corr, event)
        assert score <= 0.95

    def test_max_base_confidence_0_95(self) -> None:
        # Even without bonuses, a very high base is capped
        rule = _MockRule(base_confidence=1.0)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[])
        score = compute_confidence(rule, corr, event)
        assert score == pytest.approx(0.95, abs=1e-9)


class TestComputeConfidenceCombinations:
    def test_diff_and_timing_bonuses(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1, last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=15))
        corr = _corr(
            changes=[change],
            related_resources=[_make_resource_view(), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55 + diff=0.15 + timing=0.10 = 0.80
        assert score == pytest.approx(0.80, abs=1e-9)

    def test_recurrence_and_locality_bonuses_only(self) -> None:
        rule = _MockRule(base_confidence=0.50)
        event = _make_event(count=3)
        corr = _corr(changes=[], related_resources=[_make_resource_view()])
        score = compute_confidence(rule, corr, event)
        # base=0.50 + recurrence=0.05 + locality=0.05 = 0.60
        assert score == pytest.approx(0.60, abs=1e-9)

    def test_all_bonuses_except_locality(self) -> None:
        now = datetime.utcnow()
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=3, last_seen=now)
        change = _make_field_change(changed_at=now - timedelta(minutes=20))
        corr = _corr(
            changes=[change],
            related_resources=[_make_resource_view("a"), _make_resource_view("b")],
        )
        score = compute_confidence(rule, corr, event)
        # base=0.55 + diff=0.15 + timing=0.10 + recurrence=0.05 = 0.85
        assert score == pytest.approx(0.85, abs=1e-9)

    def test_r01_base_no_bonuses_except_locality(self) -> None:
        """Simulates R01 with no diff found (OOMKill without a recent change)."""
        rule = _MockRule(base_confidence=0.55)
        event = _make_event(count=1)
        corr = _corr(changes=[], related_resources=[_make_resource_view()])
        score = compute_confidence(rule, corr, event)
        # base=0.55 + locality=0.05 = 0.60
        assert score == pytest.approx(0.60, abs=1e-9)
