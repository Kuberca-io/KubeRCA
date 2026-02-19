"""Slack notification channel for KubeRCA.

Sends AnomalyAlert instances to a Slack webhook URL using Block Kit
message format with severity-coded colours.
"""

from __future__ import annotations

import structlog

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.events import Severity
from kuberca.notifications.manager import NotificationChannel

_log = structlog.get_logger(component="notifications.slack")

# Slack sidebar colours per severity
_SEVERITY_COLORS: dict[Severity, str] = {
    Severity.INFO: "#36a64f",
    Severity.WARNING: "#ffae42",
    Severity.ERROR: "#e01e5a",
    Severity.CRITICAL: "#800000",
}

_SEVERITY_EMOJI: dict[Severity, str] = {
    Severity.INFO: ":information_source:",
    Severity.WARNING: ":warning:",
    Severity.ERROR: ":x:",
    Severity.CRITICAL: ":rotating_light:",
}


class SlackNotificationChannel(NotificationChannel):
    """Delivers alerts to a Slack channel via an incoming webhook.

    Args:
        webhook_url: Slack incoming webhook URL
                     (e.g. ``https://hooks.slack.com/services/…``).
        timeout: HTTP request timeout in seconds. Defaults to 10.
    """

    def __init__(self, webhook_url: str, timeout: float = 10.0) -> None:
        if not webhook_url:
            raise ValueError("Slack webhook_url must not be empty")
        self._webhook_url = webhook_url
        self._timeout = timeout

    @property
    def channel_name(self) -> str:
        return "slack"

    async def send(self, alert: AnomalyAlert) -> bool:
        """Post *alert* to Slack as a Block Kit attachment.

        Returns True on HTTP 200 OK, False otherwise.
        """
        import httpx

        payload = self._build_payload(alert)
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(self._webhook_url, json=payload)
                if response.status_code == 200:
                    return True
                _log.warning(
                    "slack_unexpected_status",
                    status_code=response.status_code,
                    body=response.text[:200],
                    alert_id=alert.alert_id,
                )
                return False
        except httpx.TimeoutException:
            _log.warning("slack_request_timeout", alert_id=alert.alert_id)
            return False
        except httpx.HTTPError as exc:
            _log.warning("slack_http_error", error=str(exc), alert_id=alert.alert_id)
            return False

    def _build_payload(self, alert: AnomalyAlert) -> dict[object, object]:
        """Construct a Slack Block Kit message payload."""
        color = _SEVERITY_COLORS.get(alert.severity, "#cccccc")
        emoji = _SEVERITY_EMOJI.get(alert.severity, ":bell:")
        severity_label = alert.severity.value.upper()

        title = f"{emoji} KubeRCA Alert — {severity_label}"
        resource_info = (
            f"*Resource:* `{alert.resource_kind}/{alert.resource_name}`\n"
            f"*Namespace:* `{alert.namespace}`\n"
            f"*Reason:* {alert.reason}\n"
            f"*Events:* {alert.event_count}"
        )
        detected_at = alert.detected_at.strftime("%Y-%m-%d %H:%M:%S UTC")

        return {
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": title,
                                "emoji": True,
                            },
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": resource_info,
                            },
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Summary:*\n{alert.summary}",
                            },
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": (f"Alert ID: `{alert.alert_id}` | Detected at: {detected_at}"),
                                }
                            ],
                        },
                    ],
                }
            ]
        }
