"""Scout anomaly detector for KubeRCA.

Evaluates EventRecord streams against configurable thresholds and
rate-of-change rules. Emits AnomalyAlert when anomalies are detected.
Enforces per-(resource, reason) cooldown to suppress duplicate alerts.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import structlog

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.events import EventRecord, Severity

_logger = structlog.get_logger(component="anomaly_detector")

_DEFAULT_COOLDOWN_MINUTES: int = 15


def _parse_duration(value: str) -> timedelta:
    """Parse a duration string like '15m', '1h', '2h' into a timedelta.

    Raises ValueError if the format is invalid.
    """
    m = re.match(r"^([0-9]+)(m|h|d)$", value)
    if not m:
        raise ValueError(f"Invalid duration format: {value!r}. Expected [0-9]+(m|h|d).")
    amount = int(m.group(1))
    unit = m.group(2)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


@dataclass(frozen=True)
class ThresholdRule:
    """A threshold-based anomaly rule.

    An alert is emitted when event.count >= count_threshold for a matching
    (reason, severity) combination within the cooldown window.
    """

    reason: str  # Exact K8s event reason (e.g. "OOMKilled")
    count_threshold: int  # Minimum occurrence count to trigger
    min_severity: Severity = Severity.WARNING  # Minimum severity to consider
    summary_template: str = "Anomaly detected: {reason} on {resource_kind}/{namespace}/{name} ({count} occurrences)"


@dataclass(frozen=True)
class RateOfChangeRule:
    """A rate-of-change anomaly rule.

    An alert is emitted when the event count for a matching reason increases
    by at least delta_threshold within the observation window.
    """

    reason: str  # Exact K8s event reason
    delta_threshold: int  # Minimum increase in count to trigger
    observation_window: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    min_severity: Severity = Severity.WARNING
    summary_template: str = (
        "Rapid escalation: {reason} on {resource_kind}/{namespace}/{name} ({delta} new occurrences in {window})"
    )


@dataclass
class _RateWindow:
    """Sliding window state for a (resource, reason) rate-of-change rule."""

    first_count: int
    first_seen: datetime
    last_count: int
    last_seen: datetime


class AnomalyDetector:
    """Stateful anomaly detector for Kubernetes event streams.

    Evaluates each EventRecord against registered threshold and
    rate-of-change rules. Emits AnomalyAlert objects via a configurable
    callback when anomalies are detected.

    Per-(resource, reason) cooldown prevents alert storms.
    """

    def __init__(
        self,
        on_alert: Callable[[AnomalyAlert], None] | None = None,
        cooldown: str = "15m",
    ) -> None:
        """Create an AnomalyDetector.

        on_alert: optional synchronous callback invoked with each AnomalyAlert.
        cooldown: suppression window per (resource, reason). Default "15m".
        """
        self._on_alert = on_alert
        self._cooldown: timedelta = _parse_duration(cooldown)

        # Rules
        self._threshold_rules: list[ThresholdRule] = list(_DEFAULT_THRESHOLD_RULES)
        self._rate_rules: list[RateOfChangeRule] = list(_DEFAULT_RATE_RULES)

        # Cooldown state: maps (namespace, kind, name, reason) -> last_alert_time (UTC)
        self._cooldown_state: dict[tuple[str, str, str, str], datetime] = {}

        # Rate-of-change window state: maps (namespace, kind, name, reason) -> _RateWindow
        self._rate_windows: dict[tuple[str, str, str, str], _RateWindow] = {}

    def add_threshold_rule(self, rule: ThresholdRule) -> None:
        """Register an additional threshold rule."""
        self._threshold_rules.append(rule)

    def add_rate_rule(self, rule: RateOfChangeRule) -> None:
        """Register an additional rate-of-change rule."""
        self._rate_rules.append(rule)

    def set_cooldown(self, cooldown: str) -> None:
        """Update the global cooldown window (e.g., '10m', '1h')."""
        self._cooldown = _parse_duration(cooldown)

    def process_event(self, event: EventRecord) -> AnomalyAlert | None:
        """Evaluate an EventRecord against all registered rules.

        Returns an AnomalyAlert if a rule fires and the cooldown allows it,
        otherwise returns None. Also invokes the on_alert callback if set.
        """
        alert = self._evaluate_threshold_rules(event)
        if alert is None:
            alert = self._evaluate_rate_rules(event)

        if alert is not None:
            self._record_alert(event, alert)
            if self._on_alert is not None:
                try:
                    self._on_alert(alert)
                except Exception as exc:
                    _logger.error(
                        "alert_callback_error",
                        alert_id=alert.alert_id,
                        error=str(exc),
                    )
        return alert

    # ------------------------------------------------------------------
    # Threshold evaluation
    # ------------------------------------------------------------------

    def _evaluate_threshold_rules(self, event: EventRecord) -> AnomalyAlert | None:
        """Check threshold rules against the event."""
        for rule in self._threshold_rules:
            if rule.reason != event.reason:
                continue
            if not _severity_meets_minimum(event.severity, rule.min_severity):
                continue
            if event.count < rule.count_threshold:
                continue

            cooldown_key = (event.namespace, event.resource_kind, event.resource_name, event.reason)
            if self._in_cooldown(cooldown_key, event.last_seen):
                _logger.debug(
                    "alert_suppressed_cooldown",
                    reason=event.reason,
                    resource=f"{event.resource_kind}/{event.namespace}/{event.resource_name}",
                )
                return None

            summary = rule.summary_template.format(
                reason=event.reason,
                resource_kind=event.resource_kind,
                namespace=event.namespace,
                name=event.resource_name,
                count=event.count,
            )
            _logger.info(
                "threshold_anomaly_detected",
                reason=event.reason,
                resource=f"{event.resource_kind}/{event.namespace}/{event.resource_name}",
                count=event.count,
                threshold=rule.count_threshold,
            )
            return AnomalyAlert(
                severity=event.severity,
                resource_kind=event.resource_kind,
                resource_name=event.resource_name,
                namespace=event.namespace,
                reason=event.reason,
                summary=summary,
                detected_at=event.last_seen,
                event_count=event.count,
                source_events=[event.event_id],
            )
        return None

    # ------------------------------------------------------------------
    # Rate-of-change evaluation
    # ------------------------------------------------------------------

    def _evaluate_rate_rules(self, event: EventRecord) -> AnomalyAlert | None:
        """Check rate-of-change rules against the event."""
        for rule in self._rate_rules:
            if rule.reason != event.reason:
                continue
            if not _severity_meets_minimum(event.severity, rule.min_severity):
                continue

            window_key = (event.namespace, event.resource_kind, event.resource_name, event.reason)
            window = self._rate_windows.get(window_key)

            if window is None:
                self._rate_windows[window_key] = _RateWindow(
                    first_count=event.count,
                    first_seen=event.last_seen,
                    last_count=event.count,
                    last_seen=event.last_seen,
                )
                continue

            # Slide the window: reset if observation window has expired
            elapsed = event.last_seen - window.first_seen
            if elapsed > rule.observation_window:
                self._rate_windows[window_key] = _RateWindow(
                    first_count=event.count,
                    first_seen=event.last_seen,
                    last_count=event.count,
                    last_seen=event.last_seen,
                )
                window = self._rate_windows[window_key]
            else:
                window.last_count = event.count
                window.last_seen = event.last_seen

            delta = max(0, window.last_count - window.first_count)
            if delta < rule.delta_threshold:
                continue

            cooldown_key = (event.namespace, event.resource_kind, event.resource_name, event.reason)
            if self._in_cooldown(cooldown_key, event.last_seen):
                _logger.debug(
                    "rate_alert_suppressed_cooldown",
                    reason=event.reason,
                    resource=f"{event.resource_kind}/{event.namespace}/{event.resource_name}",
                )
                return None

            window_str = _format_timedelta(rule.observation_window)
            summary = rule.summary_template.format(
                reason=event.reason,
                resource_kind=event.resource_kind,
                namespace=event.namespace,
                name=event.resource_name,
                delta=delta,
                window=window_str,
            )
            _logger.info(
                "rate_anomaly_detected",
                reason=event.reason,
                resource=f"{event.resource_kind}/{event.namespace}/{event.resource_name}",
                delta=delta,
                window=window_str,
                threshold=rule.delta_threshold,
            )
            return AnomalyAlert(
                severity=event.severity,
                resource_kind=event.resource_kind,
                resource_name=event.resource_name,
                namespace=event.namespace,
                reason=event.reason,
                summary=summary,
                detected_at=event.last_seen,
                event_count=event.count,
                source_events=[event.event_id],
            )
        return None

    # ------------------------------------------------------------------
    # Cooldown management
    # ------------------------------------------------------------------

    def _in_cooldown(
        self,
        key: tuple[str, str, str, str],
        event_time: datetime,
    ) -> bool:
        """Return True if the (resource, reason) pair is within the cooldown window."""
        last_alert = self._cooldown_state.get(key)
        if last_alert is None:
            return False
        # Normalize both to UTC for comparison
        event_utc = _ensure_utc(event_time)
        last_utc = _ensure_utc(last_alert)
        return (event_utc - last_utc) < self._cooldown

    def _record_alert(self, event: EventRecord, alert: AnomalyAlert) -> None:
        """Update the cooldown state after an alert fires."""
        cooldown_key = (
            event.namespace,
            event.resource_kind,
            event.resource_name,
            event.reason,
        )
        self._cooldown_state[cooldown_key] = _ensure_utc(event.last_seen)

    def clear_cooldown(
        self,
        namespace: str,
        resource_kind: str,
        resource_name: str,
        reason: str,
    ) -> None:
        """Manually clear a cooldown entry. Useful for testing and operator overrides."""
        key = (namespace, resource_kind, resource_name, reason)
        self._cooldown_state.pop(key, None)

    def clear_all_cooldowns(self) -> None:
        """Clear all cooldown state."""
        self._cooldown_state.clear()


# ------------------------------------------------------------------
# Default rules matching the most frequent Kubernetes incident patterns
# ------------------------------------------------------------------

_DEFAULT_THRESHOLD_RULES: tuple[ThresholdRule, ...] = (
    ThresholdRule(
        reason="OOMKilled",
        count_threshold=3,
        min_severity=Severity.WARNING,
        summary_template="OOMKilled: {resource_kind}/{namespace}/{name} killed {count} times",
    ),
    ThresholdRule(
        reason="OOMKilling",
        count_threshold=3,
        min_severity=Severity.WARNING,
        summary_template="OOMKilling: {resource_kind}/{namespace}/{name} killing process ({count} events)",
    ),
    ThresholdRule(
        reason="BackOff",
        count_threshold=5,
        min_severity=Severity.WARNING,
        summary_template="CrashLoopBackOff: {resource_kind}/{namespace}/{name} back-off {count} times",
    ),
    ThresholdRule(
        reason="FailedScheduling",
        count_threshold=3,
        min_severity=Severity.WARNING,
        summary_template="FailedScheduling: {resource_kind}/{namespace}/{name} ({count} failures)",
    ),
    ThresholdRule(
        reason="ImagePullBackOff",
        count_threshold=3,
        min_severity=Severity.WARNING,
        summary_template="ImagePullBackOff: {resource_kind}/{namespace}/{name} ({count} failures)",
    ),
    ThresholdRule(
        reason="ErrImagePull",
        count_threshold=3,
        min_severity=Severity.WARNING,
        summary_template="ErrImagePull: {resource_kind}/{namespace}/{name} ({count} failures)",
    ),
    ThresholdRule(
        reason="FailedMount",
        count_threshold=3,
        min_severity=Severity.WARNING,
        summary_template="FailedMount: {resource_kind}/{namespace}/{name} ({count} failures)",
    ),
    ThresholdRule(
        reason="NodeNotReady",
        count_threshold=1,
        min_severity=Severity.ERROR,
        summary_template="NodeNotReady: {resource_kind}/{namespace}/{name} ({count} events)",
    ),
    ThresholdRule(
        reason="MemoryPressure",
        count_threshold=1,
        min_severity=Severity.WARNING,
        summary_template="MemoryPressure: node {resource_kind}/{namespace}/{name}",
    ),
    ThresholdRule(
        reason="DiskPressure",
        count_threshold=1,
        min_severity=Severity.WARNING,
        summary_template="DiskPressure: node {resource_kind}/{namespace}/{name}",
    ),
)

_DEFAULT_RATE_RULES: tuple[RateOfChangeRule, ...] = (
    RateOfChangeRule(
        reason="BackOff",
        delta_threshold=5,
        observation_window=timedelta(minutes=5),
        min_severity=Severity.WARNING,
        summary_template=(
            "Rapid CrashLoop escalation: {resource_kind}/{namespace}/{name} +{delta} back-offs in {window}"
        ),
    ),
    RateOfChangeRule(
        reason="OOMKilled",
        delta_threshold=3,
        observation_window=timedelta(minutes=10),
        min_severity=Severity.WARNING,
        summary_template=("Rapid OOM escalation: {resource_kind}/{namespace}/{name} +{delta} OOM kills in {window}"),
    ),
    RateOfChangeRule(
        reason="FailedScheduling",
        delta_threshold=5,
        observation_window=timedelta(minutes=5),
        min_severity=Severity.WARNING,
        summary_template=(
            "Rapid scheduling failures: {resource_kind}/{namespace}/{name} +{delta} failures in {window}"
        ),
    ),
)


# ------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------

_SEVERITY_RANK: dict[Severity, int] = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.ERROR: 2,
    Severity.CRITICAL: 3,
}


def _severity_meets_minimum(actual: Severity, minimum: Severity) -> bool:
    """Return True if actual severity is >= the minimum required severity."""
    return _SEVERITY_RANK.get(actual, 0) >= _SEVERITY_RANK.get(minimum, 0)


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure a datetime is UTC-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _format_timedelta(td: timedelta) -> str:
    """Format a timedelta as a human-readable string like '5m' or '1h'."""
    total_seconds = int(td.total_seconds())
    if total_seconds % 3600 == 0:
        return f"{total_seconds // 3600}h"
    if total_seconds % 60 == 0:
        return f"{total_seconds // 60}m"
    return f"{total_seconds}s"
