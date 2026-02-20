"""Unit tests for kuberca.notifications.webhook."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.events import Severity
from kuberca.notifications.webhook import WebhookNotificationChannel


def _make_alert() -> AnomalyAlert:
    return AnomalyAlert(
        severity=Severity.WARNING,
        resource_kind="Deployment",
        resource_name="api",
        namespace="production",
        reason="Progressing",
        summary="Deployment is not progressing.",
        detected_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        event_count=2,
    )


class TestWebhookNotificationChannel:
    def test_raises_on_empty_url(self) -> None:
        with pytest.raises(ValueError, match="url"):
            WebhookNotificationChannel(url="")

    def test_channel_name_is_webhook(self) -> None:
        ch = WebhookNotificationChannel(url="https://example.com/hook")
        assert ch.channel_name == "webhook"

    def test_build_payload_fields(self) -> None:
        ch = WebhookNotificationChannel(url="https://example.com/hook")
        alert = _make_alert()
        payload = ch._build_payload(alert)
        assert payload["alert_id"] == alert.alert_id
        assert payload["severity"] == "warning"
        assert payload["resource_kind"] == "Deployment"
        assert payload["resource_name"] == "api"
        assert payload["namespace"] == "production"
        assert payload["reason"] == "Progressing"
        assert payload["summary"] == "Deployment is not progressing."
        assert payload["detected_at"] == "2024-01-15T12:00:00+00:00"
        assert payload["event_count"] == 2
        assert isinstance(payload["source_events"], list)

    @pytest.mark.asyncio
    async def test_send_returns_true_on_2xx(self) -> None:
        ch = WebhookNotificationChannel(url="https://example.com/hook")
        alert = _make_alert()
        mock_response = MagicMock()
        mock_response.is_success = True

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await ch.send(alert)

        assert result is True

    @pytest.mark.asyncio
    async def test_send_returns_false_on_non_2xx(self) -> None:
        ch = WebhookNotificationChannel(url="https://example.com/hook")
        alert = _make_alert()
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 500
        mock_response.text = "server error"

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

        ch = WebhookNotificationChannel(url="https://example.com/hook")
        alert = _make_alert()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
            mock_client_cls.return_value = mock_client

            result = await ch.send(alert)

        assert result is False

    def test_extra_headers_are_forwarded(self) -> None:
        ch = WebhookNotificationChannel(
            url="https://example.com/hook",
            headers={"Authorization": "Bearer secret"},
        )
        assert ch._headers == {"Authorization": "Bearer secret"}
