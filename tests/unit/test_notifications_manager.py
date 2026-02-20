"""Unit tests for kuberca.notifications.manager."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.events import Severity
from kuberca.notifications.manager import (
    AlertDeduplicator,
    NotificationChannel,
    NotificationDispatcher,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_alert(
    reason: str = "CrashLoopBackOff",
    namespace: str = "default",
    resource_kind: str = "Pod",
    resource_name: str = "test-pod",
    severity: Severity = Severity.ERROR,
) -> AnomalyAlert:
    return AnomalyAlert(
        severity=severity,
        resource_kind=resource_kind,
        resource_name=resource_name,
        namespace=namespace,
        reason=reason,
        summary="Pod is crash looping.",
        detected_at=datetime.now(tz=UTC),
        event_count=3,
    )


class _FakeChannel(NotificationChannel):
    def __init__(self, name: str = "fake", success: bool = True) -> None:
        self._name = name
        self._success = success
        self.sent: list[AnomalyAlert] = []

    @property
    def channel_name(self) -> str:
        return self._name

    async def send(self, alert: AnomalyAlert) -> bool:
        self.sent.append(alert)
        return self._success


class _RaisingChannel(NotificationChannel):
    @property
    def channel_name(self) -> str:
        return "raising"

    async def send(self, alert: AnomalyAlert) -> bool:
        raise RuntimeError("channel exploded")


# ---------------------------------------------------------------------------
# AlertDeduplicator
# ---------------------------------------------------------------------------


class TestAlertDeduplicator:
    def test_first_alert_is_allowed(self) -> None:
        dedup = AlertDeduplicator()
        alert = _make_alert()
        assert dedup.should_send(alert) is True

    def test_second_alert_within_cooldown_is_suppressed(self) -> None:
        dedup = AlertDeduplicator(cooldown=timedelta(minutes=15))
        alert = _make_alert()
        assert dedup.should_send(alert) is True
        assert dedup.should_send(alert) is False

    def test_alert_after_cooldown_is_allowed(self) -> None:
        dedup = AlertDeduplicator(cooldown=timedelta(seconds=0))
        alert = _make_alert()
        assert dedup.should_send(alert) is True
        assert dedup.should_send(alert) is True

    def test_different_reason_is_independent(self) -> None:
        dedup = AlertDeduplicator()
        a1 = _make_alert(reason="CrashLoopBackOff")
        a2 = _make_alert(reason="OOMKilled")
        assert dedup.should_send(a1) is True
        assert dedup.should_send(a2) is True

    def test_different_namespace_is_independent(self) -> None:
        dedup = AlertDeduplicator()
        a1 = _make_alert(namespace="default")
        a2 = _make_alert(namespace="staging")
        assert dedup.should_send(a1) is True
        assert dedup.should_send(a2) is True

    def test_different_resource_name_is_independent(self) -> None:
        dedup = AlertDeduplicator()
        a1 = _make_alert(resource_name="pod-a")
        a2 = _make_alert(resource_name="pod-b")
        assert dedup.should_send(a1) is True
        assert dedup.should_send(a2) is True

    def test_reset_clears_cooldown(self) -> None:
        dedup = AlertDeduplicator()
        alert = _make_alert()
        assert dedup.should_send(alert) is True
        assert dedup.should_send(alert) is False
        dedup.reset(
            namespace=alert.namespace,
            resource_kind=alert.resource_kind,
            resource_name=alert.resource_name,
            reason=alert.reason,
        )
        assert dedup.should_send(alert) is True

    def test_reset_on_unknown_key_is_safe(self) -> None:
        dedup = AlertDeduplicator()
        # Must not raise
        dedup.reset("ns", "Pod", "pod", "Reason")


# ---------------------------------------------------------------------------
# NotificationDispatcher
# ---------------------------------------------------------------------------


class TestNotificationDispatcher:
    @pytest.mark.asyncio
    async def test_dispatch_sends_to_all_channels(self) -> None:
        ch1 = _FakeChannel("ch1")
        ch2 = _FakeChannel("ch2")
        dedup = AlertDeduplicator(cooldown=timedelta(seconds=0))
        dispatcher = NotificationDispatcher([ch1, ch2], dedup)
        alert = _make_alert()
        dispatcher.dispatch(alert)
        await asyncio.sleep(0.05)  # let background task complete
        assert len(ch1.sent) == 1
        assert len(ch2.sent) == 1
        assert ch1.sent[0].alert_id == alert.alert_id

    @pytest.mark.asyncio
    async def test_dispatch_deduplication_suppresses_repeat(self) -> None:
        ch = _FakeChannel()
        dedup = AlertDeduplicator(cooldown=timedelta(minutes=15))
        dispatcher = NotificationDispatcher([ch], dedup)
        alert = _make_alert()
        dispatcher.dispatch(alert)
        dispatcher.dispatch(alert)  # should be suppressed
        await asyncio.sleep(0.05)
        assert len(ch.sent) == 1

    @pytest.mark.asyncio
    async def test_dispatch_channel_exception_does_not_propagate(self) -> None:
        bad_ch = _RaisingChannel()
        good_ch = _FakeChannel("good")
        dedup = AlertDeduplicator(cooldown=timedelta(seconds=0))
        dispatcher = NotificationDispatcher([bad_ch, good_ch], dedup)
        alert = _make_alert()
        # Must not raise
        dispatcher.dispatch(alert)
        await asyncio.sleep(0.05)
        # good channel still receives the alert
        assert len(good_ch.sent) == 1

    @pytest.mark.asyncio
    async def test_dispatch_failed_channel_records_false_metric(self) -> None:
        fail_ch = _FakeChannel("fail", success=False)
        dedup = AlertDeduplicator(cooldown=timedelta(seconds=0))
        dispatcher = NotificationDispatcher([fail_ch], dedup)
        alert = _make_alert()
        with patch("kuberca.notifications.manager.notifications_total") as mock_metric:
            mock_labels = MagicMock()
            mock_metric.labels.return_value = mock_labels
            dispatcher.dispatch(alert)
            await asyncio.sleep(0.05)
        mock_metric.labels.assert_called_once_with(channel="fail", success="false")
        mock_labels.inc.assert_called_once()

    def test_dispatch_with_no_channels_is_safe(self) -> None:
        dedup = AlertDeduplicator(cooldown=timedelta(seconds=0))
        dispatcher = NotificationDispatcher([], dedup)
        alert = _make_alert()
        # Must not raise
        dispatcher.dispatch(alert)
