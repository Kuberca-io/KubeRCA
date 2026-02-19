"""R10 Readiness Probe Failure — Tier 2 rule.

Matches repeated readiness probe failure events and correlates probe
configuration changes and dependency service health.
"""

from __future__ import annotations

import re
import time
from datetime import UTC

from kuberca.models.analysis import (
    AffectedResource,
    CorrelationResult,
    EvidenceItem,
    RuleResult,
)
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r10_readiness_probe")

_PROBE_REASONS = frozenset({"Unhealthy", "ProbeError", "ProbeWarning"})

_RE_READINESS = re.compile(r"readiness probe", re.IGNORECASE)
# Readiness probe failures have count >= 3 for repeated pattern signal.
_MIN_COUNT = 3


def _is_probe_relevant(fc: FieldChange) -> bool:
    """Return True for readiness probe configuration changes."""
    path = fc.field_path.lower()
    return "readinessprobe" in path or "readiness_probe" in path


def _find_owning_deployment(
    pod_name: str,
    namespace: str,
    cache: ResourceCache,
) -> CachedResourceView | None:
    parts = pod_name.rsplit("-", 2)
    rs_prefix = parts[0] if len(parts) >= 3 else pod_name
    for rs in cache.list("ReplicaSet", namespace):
        if rs.name == rs_prefix or pod_name.startswith(rs.name + "-"):
            rs_parts = rs.name.rsplit("-", 1)
            deploy_prefix = rs_parts[0] if len(rs_parts) == 2 else rs.name  # noqa: SIM210
            for deploy in cache.list("Deployment", namespace):
                if deploy.name == deploy_prefix or rs.name.startswith(deploy.name + "-"):
                    return deploy
    return None


def _probe_details(deployment: CachedResourceView) -> str:
    """Extract probe configuration summary from deployment spec."""
    try:
        raw_template = deployment.spec.get("template", {})
        template: dict[str, object] = raw_template if isinstance(raw_template, dict) else {}
        raw_spec = template.get("spec", {})
        spec_dict: dict[str, object] = raw_spec if isinstance(raw_spec, dict) else {}
        containers = spec_dict.get("containers", [])
        if isinstance(containers, list) and containers:
            raw_first = containers[0]
            if not isinstance(raw_first, dict):
                return "<unknown probe config>"
            first: dict[str, object] = raw_first
            raw_probe = first.get("readinessProbe", {})
            if not isinstance(raw_probe, dict):
                return "<unknown probe config>"
            probe: dict[str, object] = raw_probe
            if isinstance(probe, dict):
                http = probe.get("httpGet", {})
                if isinstance(http, dict):
                    return f"httpGet path={http.get('path')} port={http.get('port')}"
                tcp = probe.get("tcpSocket", {})
                if isinstance(tcp, dict):
                    return f"tcpSocket port={tcp.get('port')}"
                exec_probe = probe.get("exec", {})
                if isinstance(exec_probe, dict):
                    return f"exec command={exec_probe.get('command')}"
    except (AttributeError, TypeError, IndexError):
        pass
    return "<unknown probe config>"


class ReadinessProbeRule(Rule):
    """Matches repeated readiness probe failures and correlates probe config changes."""

    rule_id = "R10_readiness_probe"
    display_name = "Readiness Probe Failure"
    priority = 30
    base_confidence = 0.50
    resource_dependencies = ["Pod", "Deployment", "ReplicaSet"]
    relevant_field_paths = ["spec.template.spec.containers"]

    def match(self, event: EventRecord) -> bool:
        """Return True for repeated readiness probe failure events."""
        if event.reason not in _PROBE_REASONS:
            return False
        if event.count < _MIN_COUNT:
            return False
        return bool(_RE_READINESS.search(event.message))

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find the owning Deployment and diff probe configuration changes."""
        t_start = time.monotonic()
        objects_queried = 0

        deployment = _find_owning_deployment(event.resource_name, event.namespace, cache)
        objects_queried += len(cache.list("ReplicaSet", event.namespace))
        objects_queried += len(cache.list("Deployment", event.namespace))

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        if deployment is not None:
            related_resources.append(deployment)
            raw = ledger.diff("Deployment", event.namespace, deployment.name, since_hours=2.0)
            objects_queried += 1
            for fc in raw:
                if _is_probe_relevant(fc):
                    relevant_changes.append(fc)

        duration_ms = (time.monotonic() - t_start) * 1000.0
        return CorrelationResult(
            changes=relevant_changes,
            related_events=[],
            related_resources=related_resources,
            objects_queried=objects_queried,
            duration_ms=duration_ms,
        )

    def explain(
        self,
        event: EventRecord,
        correlation: CorrelationResult,
    ) -> RuleResult:
        """Produce the RuleResult for this readiness probe failure."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(
                    f"Pod {event.resource_name} readiness probe failing (count={event.count}): {event.message[:300]}"
                ),
            )
        ]
        affected: list[AffectedResource] = [
            AffectedResource(
                kind="Pod",
                namespace=event.namespace,
                name=event.resource_name,
            )
        ]

        deploy: CachedResourceView | None = correlation.related_resources[0] if correlation.related_resources else None
        if deploy:
            affected.append(AffectedResource(kind="Deployment", namespace=deploy.namespace, name=deploy.name))

        if correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Readiness probe config changed: '{latest.field_path}' "
                        f"{latest.old_value!r} → {latest.new_value!r}"
                    ),
                )
            )
            root_cause = (
                f"Pod {event.resource_name} readiness probe is failing repeatedly "
                f"(count={event.count}). "
                f"Probe configuration was recently changed: '{latest.field_path}' "
                f"updated at {latest.changed_at.isoformat()}."
            )
        else:
            probe_info = _probe_details(deploy) if deploy else "<unknown>"
            root_cause = (
                f"Pod {event.resource_name} readiness probe is failing repeatedly "
                f"(count={event.count}). "
                f"No recent probe configuration change detected. "
                f"Current probe: {probe_info}. "
                f"The application may not be ready at the configured probe endpoint."
            )

        deploy_name = deploy.name if deploy else event.resource_name
        remediation = (
            f"Inspect probe failure details: "
            f"`kubectl describe pod {event.resource_name} -n {event.namespace}`. "
            f"Verify the readiness probe endpoint is accessible inside the container. "
            f"Consider increasing probe timeoutSeconds/failureThreshold for deployment "
            f"{deploy_name}."
        )

        _logger.info(
            "r10_match",
            pod=event.resource_name,
            namespace=event.namespace,
            confidence=confidence,
            probe_changes=len(correlation.changes),
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
