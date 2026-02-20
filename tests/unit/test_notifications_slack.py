"""Unit tests for kuberca.notifications.slack."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.events import Severity
from kuberca.notifications.slack import SlackNotificationChannel


def _make_alert(severity: Severity = Severity.ERROR) -> AnomalyAlert:
    return AnomalyAlert(
        severity=severity,
        resource_kind="Pod",
        resource_name="crash-pod",
        namespace="production",
        reason="CrashLoopBackOff",
        summary="Container keeps restarting.",
        detected_at=datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC),
        event_count=5,
    )


class TestSlackNotificationChannel:
    def test_raises_on_empty_webhook_url(self) -> None:
        with pytest.raises(ValueError, match="webhook_url"):
            SlackNotificationChannel(webhook_url="")

    def test_channel_name_is_slack(self) -> None:
        ch = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        assert ch.channel_name == "slack"

    def test_build_payload_structure(self) -> None:
        ch = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        alert = _make_alert(Severity.CRITICAL)
        payload = ch._build_payload(alert)
        assert "attachments" in payload
        attachments = payload["attachments"]
        assert len(attachments) == 1
        att = attachments[0]
        assert att["color"] == "#800000"  # critical colour
        blocks = att["blocks"]
        # header block should contain severity label
        header = blocks[0]
        assert header["type"] == "header"
        assert "CRITICAL" in header["text"]["text"]

    def test_build_payload_contains_resource_info(self) -> None:
        ch = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        alert = _make_alert()
        payload = ch._build_payload(alert)
        att = payload["attachments"][0]
        section_texts = [b["text"]["text"] for b in att["blocks"] if b.get("type") == "section"]
        combined = " ".join(section_texts)
        assert "Pod/crash-pod" in combined
        assert "production" in combined
        assert "CrashLoopBackOff" in combined
        assert "Container keeps restarting." in combined

    @pytest.mark.asyncio
    async def test_send_returns_true_on_200(self) -> None:
        ch = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        alert = _make_alert()
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await ch.send(alert)

        assert result is True

    @pytest.mark.asyncio
    async def test_send_returns_false_on_non_200(self) -> None:
        ch = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        alert = _make_alert()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "invalid_payload"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await ch.send(alert)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_returns_false_on_timeout(self) -> None:
        import httpx

        ch = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        alert = _make_alert()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
            mock_client_cls.return_value = mock_client

            result = await ch.send(alert)

        assert result is False

    def test_severity_colors_all_defined(self) -> None:
        from kuberca.notifications.slack import _SEVERITY_COLORS

        for severity in Severity:
            assert severity in _SEVERITY_COLORS, f"Missing colour for {severity}"
