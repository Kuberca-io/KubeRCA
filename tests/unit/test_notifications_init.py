"""Unit tests for kuberca.notifications.__init__ (factory and DSN parser)."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from kuberca.models.config import NotificationConfig
from kuberca.notifications import _parse_smtp_dsn, build_notification_dispatcher
from kuberca.notifications.email import EmailNotificationChannel, SMTPConfig
from kuberca.notifications.slack import SlackNotificationChannel
from kuberca.notifications.webhook import WebhookNotificationChannel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(**kwargs: str) -> NotificationConfig:
    """Construct a NotificationConfig with sensible empty defaults."""
    defaults: dict[str, str] = {
        "slack_secret_ref": "",
        "email_secret_ref": "",
        "email_to": "",
        "webhook_secret_ref": "",
    }
    defaults.update(kwargs)
    return NotificationConfig(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# TestBuildNotificationDispatcher
# ---------------------------------------------------------------------------


class TestBuildNotificationDispatcher:
    def test_no_config_yields_zero_channels(self) -> None:
        """An empty NotificationConfig must produce a dispatcher with no channels."""
        config = _config()
        dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 0

    def test_slack_only_yields_one_slack_channel(self) -> None:
        """Resolving a non-empty Slack webhook URL adds exactly one SlackNotificationChannel."""
        config = _config(slack_secret_ref="SLACK_URL")
        with patch.dict(os.environ, {"SLACK_URL": "https://hooks.slack.com/test"}):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 1
        assert isinstance(dispatcher._channels[0], SlackNotificationChannel)

    def test_email_only_yields_one_email_channel(self) -> None:
        """Resolving a non-empty SMTP DSN with a recipient adds exactly one EmailNotificationChannel."""
        config = _config(email_secret_ref="EMAIL_DSN", email_to="admin@example.com")
        dsn = "smtp://user:pass@mail.example.com:587/from@example.com"
        with patch.dict(os.environ, {"EMAIL_DSN": dsn}):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 1
        assert isinstance(dispatcher._channels[0], EmailNotificationChannel)

    def test_webhook_only_yields_one_webhook_channel(self) -> None:
        """Resolving a non-empty webhook URL adds exactly one WebhookNotificationChannel."""
        config = _config(webhook_secret_ref="WEBHOOK_URL")
        with patch.dict(os.environ, {"WEBHOOK_URL": "https://example.com/hook"}):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 1
        assert isinstance(dispatcher._channels[0], WebhookNotificationChannel)

    def test_all_three_channels_configured(self) -> None:
        """When all three secret refs resolve, the dispatcher holds exactly three channels."""
        config = _config(
            slack_secret_ref="SLACK_URL",
            email_secret_ref="EMAIL_DSN",
            email_to="admin@example.com",
            webhook_secret_ref="WEBHOOK_URL",
        )
        env = {
            "SLACK_URL": "https://hooks.slack.com/services/T000/B000/xxx",
            "EMAIL_DSN": "smtp://user:pass@mail.example.com:587/from@example.com",
            "WEBHOOK_URL": "https://example.com/hook",
        }
        with patch.dict(os.environ, env):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 3
        channel_types = {type(ch) for ch in dispatcher._channels}
        assert channel_types == {SlackNotificationChannel, EmailNotificationChannel, WebhookNotificationChannel}

    def test_empty_env_var_value_yields_zero_channels(self) -> None:
        """A secret_ref that points to an env var whose value is empty must be skipped."""
        config = _config(slack_secret_ref="SLACK_URL")
        with patch.dict(os.environ, {"SLACK_URL": ""}):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 0

    def test_invalid_slack_url_raises_value_error_gracefully(self) -> None:
        """If SlackNotificationChannel raises ValueError, the channel is skipped and no exception propagates."""
        config = _config(slack_secret_ref="SLACK_URL")
        with (
            patch.dict(os.environ, {"SLACK_URL": "not-a-valid-url"}),
            patch(
                "kuberca.notifications.SlackNotificationChannel",
                side_effect=ValueError("Slack webhook_url must be a valid URL"),
            ),
        ):
            # Must not raise — the factory catches ValueError and logs a warning
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 0

    def test_email_missing_email_to_skips_channel(self) -> None:
        """Email channel is not created when email_to is empty, even if the DSN env var is set."""
        config = _config(email_secret_ref="EMAIL_DSN", email_to="")
        with patch.dict(os.environ, {"EMAIL_DSN": "smtp://u:p@host.com:587/from@host.com"}):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 0

    def test_unset_env_var_yields_zero_channels(self) -> None:
        """A secret_ref that references an env var not present at all must be skipped."""
        config = _config(webhook_secret_ref="WEBHOOK_DOES_NOT_EXIST")
        # Ensure the env var is absent
        env_without_key = {k: v for k, v in os.environ.items() if k != "WEBHOOK_DOES_NOT_EXIST"}
        with patch.dict(os.environ, env_without_key, clear=True):
            dispatcher = build_notification_dispatcher(config)
        assert len(dispatcher._channels) == 0


# ---------------------------------------------------------------------------
# TestParseSmtpDsn
# ---------------------------------------------------------------------------


class TestParseSmtpDsn:
    def test_smtp_plain_scheme(self) -> None:
        """smtp:// DSN produces use_tls=False with the explicit port."""
        result = _parse_smtp_dsn("smtp://user:pass@host.com:587/from@host.com")
        assert isinstance(result, SMTPConfig)
        assert result.use_tls is False
        assert result.port == 587

    def test_smtps_tls_scheme(self) -> None:
        """smtps:// DSN produces use_tls=True with the explicit port."""
        result = _parse_smtp_dsn("smtps://user:pass@host.com:465/from@host.com")
        assert result.use_tls is True
        assert result.port == 465

    def test_default_port_for_smtp(self) -> None:
        """smtp:// with no port defaults to 587."""
        result = _parse_smtp_dsn("smtp://user:pass@host.com/from@host.com")
        assert result.port == 587

    def test_default_port_for_smtps(self) -> None:
        """smtps:// with no port defaults to 465."""
        result = _parse_smtp_dsn("smtps://user:pass@host.com/from@host.com")
        assert result.port == 465

    def test_invalid_scheme_raises_value_error(self) -> None:
        """A DSN that does not start with smtp:// or smtps:// must raise ValueError."""
        with pytest.raises(ValueError, match="smtp://"):
            _parse_smtp_dsn("http://host.com")

    def test_from_addr_extracted_from_path(self) -> None:
        """The path segment (with leading slash stripped) becomes from_addr."""
        result = _parse_smtp_dsn("smtp://user:pass@host.com:587/sender@example.com")
        assert result.from_addr == "sender@example.com"

    def test_credentials_parsed_correctly(self) -> None:
        """Username and password from the authority component are extracted into SMTPConfig."""
        result = _parse_smtp_dsn("smtp://myuser:mypassword@mail.example.com:587/noreply@example.com")
        assert result.username == "myuser"
        assert result.password == "mypassword"

    def test_host_parsed_correctly(self) -> None:
        """Hostname from the authority component is extracted into SMTPConfig."""
        result = _parse_smtp_dsn("smtp://u:p@mail.example.com:587/from@example.com")
        assert result.host == "mail.example.com"

    def test_smtp_plain_scheme_full_fields(self) -> None:
        """All SMTPConfig fields are populated correctly from a complete smtp:// DSN."""
        result = _parse_smtp_dsn("smtp://user:pass@mail.example.com:587/from@example.com")
        assert result.host == "mail.example.com"
        assert result.port == 587
        assert result.username == "user"
        assert result.password == "pass"
        assert result.from_addr == "from@example.com"
        assert result.use_tls is False
        assert result.timeout == 10.0

    def test_smtps_tls_scheme_full_fields(self) -> None:
        """All SMTPConfig fields are populated correctly from a complete smtps:// DSN."""
        result = _parse_smtp_dsn("smtps://user:pass@mail.example.com:465/from@example.com")
        assert result.host == "mail.example.com"
        assert result.port == 465
        assert result.username == "user"
        assert result.password == "pass"
        assert result.from_addr == "from@example.com"
        assert result.use_tls is True
        assert result.timeout == 10.0
