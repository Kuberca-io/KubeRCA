"""Unit tests for kuberca.notifications.email."""

from __future__ import annotations

import smtplib
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.events import Severity
from kuberca.notifications.email import EmailNotificationChannel, SMTPConfig


def _make_smtp_config(**kwargs: object) -> SMTPConfig:
    defaults: dict[str, object] = {
        "host": "smtp.example.com",
        "port": 587,
        "username": "user@example.com",
        "password": "secret",
        "from_addr": "alerts@example.com",
    }
    defaults.update(kwargs)
    return SMTPConfig(**defaults)  # type: ignore[arg-type]


def _make_alert(severity: Severity = Severity.CRITICAL) -> AnomalyAlert:
    return AnomalyAlert(
        severity=severity,
        resource_kind="Node",
        resource_name="node-01",
        namespace="",
        reason="NotReady",
        summary="Node has been NotReady for 5 minutes.",
        detected_at=datetime(2024, 3, 10, 8, 0, 0, tzinfo=UTC),
        event_count=1,
    )


class TestSMTPConfig:
    def test_raises_on_empty_host(self) -> None:
        with pytest.raises(ValueError, match="host"):
            SMTPConfig(host="", port=587, username="u", password="p", from_addr="a@b.com")

    def test_raises_on_empty_from_addr(self) -> None:
        with pytest.raises(ValueError, match="from_addr"):
            SMTPConfig(host="smtp.x.com", port=587, username="u", password="p", from_addr="")


class TestEmailNotificationChannel:
    def test_raises_on_empty_to_addr(self) -> None:
        cfg = _make_smtp_config()
        with pytest.raises(ValueError, match="to_addr"):
            EmailNotificationChannel(smtp_config=cfg, to_addr="")

    def test_channel_name_is_email(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        assert ch.channel_name == "email"

    def test_build_message_subject_contains_severity_and_reason(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        alert = _make_alert()
        msg = ch._build_message(alert)
        subject = msg["Subject"]
        assert "CRITICAL" in subject
        assert "NotReady" in subject
        assert "node-01" in subject

    def test_build_message_has_html_and_plain_parts(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        alert = _make_alert()
        msg = ch._build_message(alert)
        payloads = [part.get_payload(decode=True).decode() for part in msg.get_payload()]  # type: ignore[union-attr]
        assert any("<!DOCTYPE html>" in p for p in payloads), "Missing HTML part"
        assert any("KubeRCA Alert" in p and "<!DOCTYPE" not in p for p in payloads), "Missing plain text part"

    def test_html_contains_severity_color(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        alert = _make_alert(Severity.CRITICAL)
        html = ch._build_html(alert, "CRITICAL")
        assert "#4a0000" in html  # critical background colour

    @pytest.mark.asyncio
    async def test_send_returns_true_on_success(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        alert = _make_alert()
        with patch.object(ch, "_send_sync", return_value=None):
            result = await ch.send(alert)
        assert result is True

    @pytest.mark.asyncio
    async def test_send_returns_false_on_smtp_error(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        alert = _make_alert()
        with patch.object(ch, "_send_sync", side_effect=smtplib.SMTPException("auth failed")):
            result = await ch.send(alert)
        assert result is False

    @pytest.mark.asyncio
    async def test_send_returns_false_on_connection_error(self) -> None:
        ch = EmailNotificationChannel(
            smtp_config=_make_smtp_config(),
            to_addr="ops@example.com",
        )
        alert = _make_alert()
        with patch.object(ch, "_send_sync", side_effect=OSError("connection refused")):
            result = await ch.send(alert)
        assert result is False
