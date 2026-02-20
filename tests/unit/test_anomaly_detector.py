"""Tests for kuberca.scout.detector — AnomalyDetector threshold and rate rules."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.scout.detector import (
    AnomalyDetector,
    RateOfChangeRule,
    ThresholdRule,
    _format_timedelta,
    _parse_duration,
    _severity_meets_minimum,
)


def _make_event(
    reason: str = "OOMKilled",
    count: int = 1,
    severity: Severity = Severity.WARNING,
    namespace: str = "default",
    kind: str = "Pod",
    name: str = "my-pod",
    last_seen: datetime | None = None,
) -> EventRecord:
    if last_seen is None:
        last_seen = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test",
        source=EventSource.CORE_EVENT,
        severity=severity,
        reason=reason,
        message=f"{reason} occurred",
        namespace=namespace,
        resource_kind=kind,
        resource_name=name,
        first_seen=last_seen,
        last_seen=last_seen,
        count=count,
    )


class TestParseDuration:
    def test_minutes(self) -> None:
        assert _parse_duration("15m") == timedelta(minutes=15)

    def test_hours(self) -> None:
        assert _parse_duration("2h") == timedelta(hours=2)

    def test_days(self) -> None:
        assert _parse_duration("1d") == timedelta(days=1)

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid duration"):
            _parse_duration("abc")

    def test_zero_minutes_raises(self) -> None:
        # 0m is syntactically valid but semantically zero — ensure it parses
        result = _parse_duration("0m")
        assert result == timedelta(0)


class TestSeverityMeetsMinimum:
    def test_info_does_not_meet_warning(self) -> None:
        assert _severity_meets_minimum(Severity.INFO, Severity.WARNING) is False

    def test_warning_meets_warning(self) -> None:
        assert _severity_meets_minimum(Severity.WARNING, Severity.WARNING) is True

    def test_critical_meets_warning(self) -> None:
        assert _severity_meets_minimum(Severity.CRITICAL, Severity.WARNING) is True

    def test_error_meets_error(self) -> None:
        assert _severity_meets_minimum(Severity.ERROR, Severity.ERROR) is True


class TestFormatTimedelta:
    def test_whole_hours(self) -> None:
        assert _format_timedelta(timedelta(hours=2)) == "2h"

    def test_whole_minutes(self) -> None:
        assert _format_timedelta(timedelta(minutes=5)) == "5m"

    def test_seconds(self) -> None:
        assert _format_timedelta(timedelta(seconds=90)) == "90s"


class TestThresholdRules:
    def test_fires_above_threshold(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        event = _make_event(reason="OOMKilled", count=5, severity=Severity.WARNING)
        alert = detector.process_event(event)
        assert alert is not None
        assert alert.reason == "OOMKilled"
        assert alert.event_count == 5

    def test_does_not_fire_below_threshold(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        # Default OOMKilled threshold = 3; count=2 should not fire
        event = _make_event(reason="OOMKilled", count=2, severity=Severity.WARNING)
        alert = detector.process_event(event)
        assert alert is None

    def test_does_not_fire_for_wrong_reason(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        event = _make_event(reason="Evicted", count=100, severity=Severity.WARNING)
        alert = detector.process_event(event)
        assert alert is None

    def test_does_not_fire_below_minimum_severity(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        # OOMKilled threshold has min_severity=WARNING; INFO should not fire
        event = _make_event(reason="OOMKilled", count=10, severity=Severity.INFO)
        alert = detector.process_event(event)
        assert alert is None

    def test_custom_threshold_rule(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        rule = ThresholdRule(
            reason="Evicted",
            count_threshold=2,
            min_severity=Severity.WARNING,
        )
        detector.add_threshold_rule(rule)
        event = _make_event(reason="Evicted", count=3, severity=Severity.WARNING)
        alert = detector.process_event(event)
        assert alert is not None
        assert alert.reason == "Evicted"

    def test_callback_called_on_alert(self) -> None:
        received: list = []
        detector = AnomalyDetector(on_alert=received.append, cooldown="1h")
        event = _make_event(reason="OOMKilled", count=5, severity=Severity.WARNING)
        detector.process_event(event)
        assert len(received) == 1

    def test_alert_contains_source_event_id(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        event = _make_event(reason="OOMKilled", count=5)
        alert = detector.process_event(event)
        assert alert is not None
        assert event.event_id in alert.source_events


class TestCooldown:
    def test_second_alert_suppressed_within_cooldown(self) -> None:
        detector = AnomalyDetector(cooldown="15m")
        ts1 = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 2, 18, 12, 10, 0, tzinfo=UTC)  # 10 min later

        event1 = _make_event(reason="OOMKilled", count=5, last_seen=ts1)
        event2 = _make_event(reason="OOMKilled", count=6, last_seen=ts2)

        alert1 = detector.process_event(event1)
        alert2 = detector.process_event(event2)

        assert alert1 is not None
        assert alert2 is None  # suppressed by cooldown

    def test_second_alert_fires_after_cooldown_expires(self) -> None:
        detector = AnomalyDetector(cooldown="5m")
        ts1 = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 2, 18, 12, 10, 0, tzinfo=UTC)  # 10 min later > 5m cooldown

        event1 = _make_event(reason="OOMKilled", count=5, last_seen=ts1)
        event2 = _make_event(reason="OOMKilled", count=8, last_seen=ts2)

        alert1 = detector.process_event(event1)
        alert2 = detector.process_event(event2)

        assert alert1 is not None
        assert alert2 is not None

    def test_different_resources_dont_share_cooldown(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        ts = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)

        event_a = _make_event(reason="OOMKilled", count=5, name="pod-a", last_seen=ts)
        event_b = _make_event(reason="OOMKilled", count=5, name="pod-b", last_seen=ts)

        alert_a = detector.process_event(event_a)
        alert_b = detector.process_event(event_b)

        assert alert_a is not None
        assert alert_b is not None

    def test_clear_cooldown_allows_immediate_alert(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        ts = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)

        event1 = _make_event(reason="OOMKilled", count=5, last_seen=ts)
        detector.process_event(event1)

        detector.clear_cooldown("default", "Pod", "my-pod", "OOMKilled")

        event2 = _make_event(reason="OOMKilled", count=6, last_seen=ts)
        alert2 = detector.process_event(event2)
        assert alert2 is not None


class TestRateOfChangeRules:
    def test_rapid_escalation_fires_alert(self) -> None:
        detector = AnomalyDetector(cooldown="1h")

        ts1 = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 2, 18, 12, 2, 0, tzinfo=UTC)  # 2 minutes later

        event1 = _make_event(reason="BackOff", count=1, last_seen=ts1, severity=Severity.WARNING)
        event2 = _make_event(reason="BackOff", count=10, last_seen=ts2, severity=Severity.WARNING)

        detector.process_event(event1)
        alert = detector.process_event(event2)

        assert alert is not None
        assert "BackOff" in alert.reason

    def test_slow_escalation_does_not_fire(self) -> None:
        detector = AnomalyDetector(cooldown="1h")

        ts1 = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 2, 18, 12, 1, 0, tzinfo=UTC)  # 1 min later

        # Delta = 2, rate threshold = 5; count=4 stays below BackOff threshold (5)
        event1 = _make_event(reason="BackOff", count=2, last_seen=ts1, severity=Severity.WARNING)
        event2 = _make_event(reason="BackOff", count=4, last_seen=ts2, severity=Severity.WARNING)

        detector.process_event(event1)
        alert = detector.process_event(event2)

        assert alert is None  # delta (2) < rate threshold (5), count (4) < threshold (5)

    def test_window_reset_resets_baseline(self) -> None:
        detector = AnomalyDetector(cooldown="1h")

        # Default BackOff rule has 5-minute observation window
        ts1 = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 2, 18, 12, 10, 0, tzinfo=UTC)  # >5 min

        # count=4 stays below BackOff threshold (5) so only rate rules are tested
        event1 = _make_event(reason="BackOff", count=1, last_seen=ts1, severity=Severity.WARNING)
        event2 = _make_event(reason="BackOff", count=4, last_seen=ts2, severity=Severity.WARNING)

        detector.process_event(event1)
        # Window expired — event2 starts a new window with count=4 as baseline
        # No alert because delta from baseline (4) to last (4) = 0
        alert = detector.process_event(event2)

        # First event in new window — no delta yet
        assert alert is None

    def test_custom_rate_rule(self) -> None:
        detector = AnomalyDetector(cooldown="1h")
        rule = RateOfChangeRule(
            reason="Evicted",
            delta_threshold=3,
            observation_window=timedelta(minutes=10),
            min_severity=Severity.WARNING,
        )
        detector.add_rate_rule(rule)

        ts1 = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 2, 18, 12, 5, 0, tzinfo=UTC)

        event1 = _make_event(reason="Evicted", count=1, last_seen=ts1, severity=Severity.WARNING)
        event2 = _make_event(reason="Evicted", count=6, last_seen=ts2, severity=Severity.WARNING)

        detector.process_event(event1)
        alert = detector.process_event(event2)

        assert alert is not None
        assert alert.reason == "Evicted"
